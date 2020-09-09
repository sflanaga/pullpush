// #![allow(dead_code)]
// #![allow(unused_imports)]
// #![allow(unused_variables)]
// #![allow(unused_mut)]
// #![allow(unreachable_code)]

use std::io::{BufReader, BufWriter};
use std::path::{PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::{Builder, sleep, spawn};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{anyhow, Context};
use crossbeam_channel::{Receiver, Sender};
use lazy_static::lazy_static;
use log::{debug, error, info, trace, warn};
use structopt::StructOpt;
use url::Url;

use sema::Semaphore;
use track::Tracker;
use vfs::{FileStatus, Vfs};

use crate::cli::Cli;
use crate::track::TrackDelta;

mod cli;
mod track;
mod copier;
mod vfs;
mod fast_stat;
mod sema;
mod util;

#[derive(Debug)]
pub struct Stats {
    pub first_xfer_time: Mutex<Option<Instant>>,
    pub xfer_count: AtomicUsize,
    pub dirs_check: AtomicUsize,
    pub path_check: AtomicUsize,
    pub stat_check: AtomicUsize,
    pub never2xfer: AtomicUsize,
    pub too_young: AtomicUsize,
}

lazy_static! {
    pub static ref STATS: Stats = Stats {
        first_xfer_time: Mutex::new(None),
        xfer_count: AtomicUsize::new(0),
        dirs_check: AtomicUsize::new(0),
        path_check: AtomicUsize::new(0),
        stat_check: AtomicUsize::new(0),
        never2xfer: AtomicUsize::new(0),
        too_young: AtomicUsize::new(0),
    };

    pub static ref SSH_SEMA: Semaphore = Semaphore::new(0);
}

type Result<T> = anyhow::Result<T, anyhow::Error>;

fn main() {
    if let Err(err) = run() {
        error!("Error: {}\n{:?}\n{:#?}", &err, &err, &err);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Arc::new({
        let mut cli = Cli::from_args();
        check_url(&cli.src_url)?;
        check_url(&cli.dst_url)?;
        cli
    });

    util::init_log(cli.log_level);

    for _ in 0..cli.number_of_ssh_startups {
        SSH_SEMA.release();
    }

    let src = vfs::Vfs::new(&cli.src_url, cli.dst_perm, &cli.src_pk, Some(cli.timeout))?;
    // we do not use this dst but it is done to make sure the downstream can connect before too much machinery
    // get going.  Might be removed later.
    let _dst = vfs::Vfs::new(&cli.dst_url, cli.dst_perm, &cli.dst_pk, Some(cli.timeout))?;

    let tracker = Arc::new(RwLock::new(Tracker::new(&cli.track, cli.max_track_age)?));

    let (send, recv) = crossbeam_channel::unbounded();

    let mut xfer_threads = vec![];
    for i in 0..cli.threads {
        let recv_c = recv.clone();
        let cli_c = cli.clone();
        let mut tracker_c = tracker.clone();

        let h = Builder::new().name(format!("{}:{}", "xfer", i)).spawn(move || xferring(&recv_c, &cli_c, &mut tracker_c)).unwrap();
        xfer_threads.push(h);
    }

    let start = Instant::now();

    debug!("listing source");

    let tic_dur = cli.ticker_interval;
    let _h_tic = spawn(move || ticker(tic_dur));

    let h_lister_thread = {
        let (cli_c, tracker_c, send_c) = (cli.clone(), tracker.clone(), send.clone());
        debug!("starting lister thread");
        Builder::new().name("lister".to_string()).spawn(move || lister_thread(&cli_c, src, &tracker_c, &send_c)).context("lister thread start failed")?
    };
    trace!("lister has started");
    let l_s = h_lister_thread.join().unwrap()?;

    info!("paths list {} in {:?}  total {:?}", l_s.paths_listed, l_s.dir_list_time, l_s.total_time);
    info!("paths filtered in {:?}  files stat'ed {} / {:?}", l_s.path_filter_time, l_s.paths_stat_ed, l_s.stat_filter_time);
    info!("paths q'ed {} in {:?}", l_s.paths_queued, l_s.queue_after_time);
    info!("write(s) all to tracker: {} in {:?}", l_s.add_all_to_tracker, l_s.add_all_to_tracker_time);

    let mut count = 0u64;
    let mut size = 0u64;
    for _ in &xfer_threads {
        send.send(None)?;
    }
    for h in xfer_threads {
        let (c, s) = h.join().unwrap();
        count += c;
        size += s;
    }

    let mb = (size as f64) / (1024.0 * 1024.0);

    let first_xfer = STATS.first_xfer_time.lock().unwrap().take();
    if first_xfer.is_some() {
        let first_xfer = first_xfer.unwrap();
        let rate = size as f64 / first_xfer.elapsed().as_secs_f64();
        info!("transferred {} files {:.3} MB in {:.3} secs NOT counting list time Rate: {:.3}MB/s", count, mb, first_xfer.elapsed().as_secs_f64(), rate/(1024.0*1024.0));
        debug!("xfer time: {:.3}", first_xfer.elapsed().as_secs_f64());
    } else {
        info!("transferred {} files {:.3} MB in {:.3} secs counting list time", count, mb, start.elapsed().as_secs_f64());
    }
    tracker.write().unwrap().commit()?;

    debug!("STATS: {:#?}", *STATS);

    Ok(())
}


fn check_url(url: &Url) -> Result<()> {
    if url.scheme() == "sftp" {
        if url.port().is_none() { return Err(anyhow!("Url MUST set port explicitly: {}", &url)); }
        if url.username().len() == 0 { return Err(anyhow!("Url MUST set username explicitly: {}", &url)); }
        Ok(())
    } else if url.scheme() == "file" {
        Ok(())
    } else {
        Err(anyhow!("Scheme \"{}\" not handled in url: {}", url.scheme(), &url))?
    }
}

fn xferring(recv_c: &Receiver<Option<(PathBuf, FileStatus)>>, cli_c: &Arc<Cli>, tracker: &mut Arc<RwLock<Tracker>>) -> (u64, u64) {
    match xferring_inn(recv_c, cli_c, tracker) {
        Err(e) => {
            error!("sending thread died: {:#?} - maybe the others will get it down this round", e);
            (0, 0)
        }
        Ok(x) => x,
    }
}

fn xferring_inn(recv_c: &Receiver<Option<(PathBuf, FileStatus)>>, cli: &Arc<Cli>, tracker: &mut Arc<RwLock<Tracker>>) -> Result<(u64, u64)> {
    let (src,dst) = {
        let _l = SSH_SEMA.access();
        (vfs::Vfs::new(&cli.src_url, cli.dst_perm, &cli.src_pk, Some(cli.timeout))?, vfs::Vfs::new(&cli.dst_url, cli.dst_perm, &cli.dst_pk, Some(cli.timeout))?)
    };

    let mut count = 0u64;
    let mut size = 0u64;
    let mut rec_1st_xfer_time = false;
    loop {
        let p = recv_c.recv().context("receiving next entry in channel")?;
        match p {
            None => return Ok((count, size)),
            Some((path, filestat)) => {
                // record the first a file start xferring - for better xfer rate stats laters
                if !rec_1st_xfer_time {
                    let mut l = STATS.first_xfer_time.lock().unwrap();
                    if l.is_none() {
                        l.replace(Instant::now());
                        trace!("replace first start");
                    }
                    rec_1st_xfer_time = true;
                }
                let (c, s) = xfer_file(&cli, &path, &src, &dst)?;
                STATS.xfer_count.fetch_add(1, Ordering::Relaxed);
                size += s;
                count += c;
                tracker.write().unwrap().xferred(&path, filestat)?;
            }
        }
    }
    // Ok((count, size))
}

fn xfer_file(cli_c: &Arc<Cli>, path: &PathBuf, src: &Vfs, dst: &Vfs) -> Result<(u64, u64)> {

    let start_dst_chk = Instant::now();

    let mut dst_path = PathBuf::from(cli_c.dst_url.path());
    let mut tmp_path = PathBuf::from(cli_c.dst_url.path());
    let name = path.file_name().unwrap().to_str().unwrap();
    let tmpname = format!(".tmp{}", name);
    dst_path.push(&name[..]);
    tmp_path.push(&tmpname[..]);

    match dst.stat(&dst_path) {
        Err(_) => (), // silencing useless info... for now warn!("continue with error during stat of dest remote \"{}\", {}", &dst_path.display(), e),
        Ok(_) => {
            if cli_c.disable_overwrite {
                warn!("file: \"{}\" already at {} and recording it as xferred - no overwrite so skipping", &path.file_name().unwrap().to_string_lossy(), &cli_c.dst_url);
                return Ok((0, 0));
            } else {
                warn!("overwriting changed file: \"{}\" already at {} ", &path.file_name().unwrap().to_string_lossy(), &cli_c.dst_url);
            }
        }
    }
    let start_open = Instant::now();
    let dst_chk_time = start_open.duration_since(start_dst_chk);


    let (time_xfer, open_time, size) = if !cli_c.threaded_copy {
        let mut f_in = BufReader::with_capacity(cli_c.copy_buffer_size, src.open(&path).with_context(|| format!("opening src file direct: {}", path.display()))?);
        let mut f_out = BufWriter::with_capacity(cli_c.copy_buffer_size,
                                                 dst.create(&tmp_path).context("opening dst file direct")?);
        let time_xfer = Instant::now();
        let open_time = time_xfer.duration_since(start_open);

        (time_xfer, open_time, std::io::copy(&mut f_in, &mut f_out)? as usize)
    } else {
        let mut f_in = Arc::new(Mutex::new(src.open(&path).with_context(|| format!("opening src file direct: {}", path.display()))?));// as Arc<Mutex<Box<dyn Read + Send>>>;
        let mut f_out =Arc::new(Mutex::new(dst.create(&tmp_path).context("opening dst file direct")?));// as Arc<Mutex<Box<dyn Write + Send>>>;

        let time_xfer = Instant::now();
        let open_time = time_xfer.duration_since(start_open);

        (time_xfer, open_time, copier::copier(&mut f_in, &mut f_out, cli_c.copy_buffer_size, cli_c.buffer_ring_size)?)
    };

    let start_rename = Instant::now();
    let xfer_time = start_rename.duration_since(time_xfer);

    match dst.rename(&tmp_path, &dst_path) {
        Err(e) => error!("Cannot rename remote tmp to final: \"{}\" to \"{}\" due to {:?}", &tmp_path.display(), &dst_path.display(), e),
        Ok(()) => {
            let rename_time = start_rename.elapsed();
            let t = xfer_time.as_secs_f64();
            let r = (size as f64) / t;
            info!("xferred: \"{}\" to {} \"{}\"  size: {}  rate: {:.3}MB/s  chk_time: {:?} open time: {:?} xfer_time: {:?} mv_time: {:?}",
                  path.display(), &cli_c.dst_url, &path.file_name().unwrap().to_string_lossy(),
                  size, r / (1024f64 * 1024f64), dst_chk_time, open_time, xfer_time, rename_time);
            if let Err(e) = dst.set_perm(&dst_path) {
                error!("could not set dst permissions for {} due to {}", dst_path.display(), e);
            }

        }
    }


    Ok((1, size as u64))
}

fn get_file_age(path: &PathBuf, filestat: &FileStatus) -> Duration {
    match SystemTime::now().duration_since(filestat.mtime) {
        Err(e) => {
            warn!("got \"future\" time for path \"{}\", so assuming 0 age.  {:#?}", path.display(), &e);
            Duration::from_secs(0)
        } // pretend its now
        Ok(dur) => dur,
    }
}

fn keep_path(cli: &Arc<Cli>, path: &PathBuf, tracker: &Arc<RwLock<Tracker>>) -> bool {
    STATS.path_check.fetch_add(1, Ordering::Relaxed);

    let s = match path.file_name() {
        None => {
            error!("Cannot map path to a filename - weird \"{}\"", &path.display());
            return false;
        }
        Some(s) => s.to_string_lossy(),
    };

    if !cli.re.is_match(&s.as_bytes()).expect("RE checked failed in keep_path") {
        trace!("file \"{}\" does not match RE", s);
        return false;
    }

    if s.starts_with('.') && !cli.include_dot_files {
        trace!("file \"{}\" excluded as a dot file or hidden", &path.display());
        return false;
    }

    if cli.disable_overwrite {
        // we only exclude on path check IF we are NOT in overwrite mode
        // yes this slows things down for NFS/NAS sources, but we must do it
        // for safest default path
        if tracker.read().expect("Unable to read lock track for path check").path_exists_in_tracker(&path) {
            trace!("file \"{}\" already in tracker", &path.display());
            return false;
        } else {
            trace!("file \"{}\" not already in tracker", &path.display());
            return true;
        }
    } else {
        trace!("file overwrite enable so stat check is needed for \"{}\"", &path.display());
        return true;
    }
}

const FILE_TOO_OLD: u32 = 1;
const FILE_TOO_YOUNG: u32 = 2;
const FILE_NOT_A_FILE: u32 = 4;
const SRC_FILE_NOT_CHANGED: u32 = 8;

fn keep_status(cli: &Arc<Cli>, path: &PathBuf, filestatus: FileStatus, tracker: &Arc<RwLock<Tracker>>) -> Result<u32> {
    STATS.stat_check.fetch_add(1, Ordering::Relaxed);

    if filestatus.file_type == vfs::FileType::Regular {
        let age = get_file_age(&path, &filestatus);
        if age > cli.max_age {
            trace!("file \"{}\" too old at {:?}", &path.display(), age);
            return Ok(FILE_TOO_OLD);
        } else if age < cli.min_age {
            trace!("file \"{}\" too new at {:?}", &path.display(), age);
            return Ok(FILE_TOO_YOUNG);
        } else if !cli.disable_overwrite {
            match tracker.read().expect("could not lock reader in keep_status").check(&path, filestatus)? {
                TrackDelta::SizeChange => {
                    info!("src file changed size: \"{}\"",path.display());
                    Ok(0)
                },
                TrackDelta::LastModChange => {
                    info!("src changed mod time: \"{}\"", path.display());
                    Ok(0)
                },
                TrackDelta::None => Ok(0),
                _ => Ok(SRC_FILE_NOT_CHANGED)
            }
        } else {
            Ok(0)
        }
    } else {
        trace!("dir: {}", &path.display());
        Ok(FILE_NOT_A_FILE)
    }

}

fn lister_thread(cli: &Arc<Cli>, src: Vfs, tracker: &Arc<RwLock<Tracker>>, send: &Sender<Option<(PathBuf, FileStatus)>>) -> Result<ListResults> {
    match inner_lister_thread(cli, src, tracker, send) {
        Err(e) => {
            error!("lister thread failed: {:?}", e);
            return Err(e);
        }
        Ok(list_stats) => return Ok(list_stats),
    }
}

struct ListResults {
    pub paths_listed: u64,
    pub dir_list_time: Duration,

    pub path_filter_time: Duration,
    pub stat_filter_time: Duration,
    pub queue_after_time: Duration,
    pub add_all_to_tracker_time: Duration,

    pub paths_stat_ed: u64,
    pub paths_queued: u64,
    pub add_all_to_tracker: u64,
    pub total_time: Duration,
}

fn inner_lister_thread(cli: &Arc<Cli>, mut src: Vfs, tracker: &Arc<RwLock<Tracker>>, send: &Sender<Option<(PathBuf, FileStatus)>>) -> Result<ListResults> {

    let mut stats = ListResults{
        dir_list_time: Default::default(),
        paths_listed: 0,
        path_filter_time: Default::default(),
        paths_stat_ed: 0,
        stat_filter_time: Default::default(),
        queue_after_time: Default::default(),
        add_all_to_tracker_time: Default::default(),
        total_time: Default::default(),
        paths_queued: 0,
        add_all_to_tracker: 0
    };

    let start_f = Instant::now();
    let dir_path = &PathBuf::from(cli.src_url.path());
    trace!("opening dir: {}", dir_path.display());
    let mut dir = src.open_dir(&dir_path).with_context(|| format!("open dir on base directory: {}", dir_path.display()))?;

    let list = &dir.read_all_dir_entry().context("error on next_dir_entry")?;
    stats.dir_list_time = start_f.elapsed();
    stats.paths_listed = list.len() as u64;

    info!("file list {} in {:?}", list.len(), start_f.elapsed());

    let mut xfer_list = vec![];
    let mut with_stat_list = vec![];

    let has_stat = list.len() > 0 && list[0].1.is_some();

    let start_path_filter = Instant::now();

    // this check is faster so done in list
    let list = if !has_stat {
        let start_f = Instant::now();
        let mut path_checked_list = list.iter()
            .map(|(p, o)| (dir_path.join(&p), o))
            .filter(|(p, _o)| keep_path(cli, p, tracker))
            .map(|(p, _o)| p).collect::<Vec<_>>();
        info!("path based checks of {} in {:?}", list.len(), start_f.elapsed());
        let start_f = Instant::now();
        let x = fast_stat::get_stats_fast(cli.local_file_stat_thread_pool_size, &mut path_checked_list).context("get fast stats failure")?;
        info!("fast file stat of {} in {:?}", x.len(), start_f.elapsed());
        x
    } else {
        list.iter().map(|(p,o)| (dir_path.join(p).clone(), o.unwrap().clone()))
            .filter(|(p, _o)| keep_path(cli, p, tracker))
            .collect::<Vec<_>>()
    };

    stats.path_filter_time = start_path_filter.elapsed();

    // this check can be slower so option to send as we find
    let start_stat_filter = Instant::now();
    for (path, filestatus) in list.iter() {
        let k_s = keep_status(&cli, &path, *filestatus, &tracker)?;
        stats.paths_stat_ed +=1;
        if k_s & FILE_NOT_A_FILE != 0 || k_s & FILE_TOO_OLD != 0 {
            // these file should never be transferred in the future
            STATS.never2xfer.fetch_add(1, Ordering::Relaxed);
            with_stat_list.push((path.clone(), filestatus));
        } else if k_s & FILE_TOO_YOUNG != 0 {
            STATS.too_young.fetch_add(1, Ordering::Relaxed);
            // do nothing but it will show up again and be old enough
            // and should be xferred
        } else if k_s & SRC_FILE_NOT_CHANGED != 0 {
            trace!("path stats have not changed: \"{}\"", path.display());
        } else {
            if !cli.dry_run {
                if !cli.disable_queue_as_found {
                    trace!("queueing file: {}", path.display());
                    send.send(Some((path.clone(), *filestatus)))?;
                } else {
                    xfer_list.push((path.clone(), filestatus.clone()));
                    stats.paths_queued += 1;
                }
            } else {
                trace!("would have xferred file: {}", path.display());
            }
        }
    }
    stats.stat_filter_time = start_stat_filter.elapsed();

    let start_queue_time = Instant::now();
    if cli.disable_queue_as_found {
        trace!("queueing all files for xfer at once");
        let start_f = Instant::now();
        let count = xfer_list.len();
        loop {
            match xfer_list.pop() {
                None => break,
                Some(x) => {
                    trace!("queueing file: {}", x.0.display());
                    stats.paths_queued += 1;
                    send.send(Some(x))?
                }
            }
        }
        info!("vec to queue {} in: {:?}", count, start_f.elapsed());
    }
    stats.queue_after_time = start_queue_time.elapsed();

    let start_add_all_to_filter = Instant::now();
    if cli.add_all_to_tracker {
        trace!("write just paths to track for later speeder listings");
        stats.add_all_to_tracker = with_stat_list.len() as u64;
        let start_f = Instant::now();
        loop {
            match with_stat_list.pop() {
                None => break,
                Some(x) => {
                    tracker.write().unwrap().insert_path_and_status(&x.0, *x.1)?
                },
            }
        }
        info!("vec of ignorable in future files {} to tracker in {:?}", stats.add_all_to_tracker, start_f.elapsed());
    }

    stats.add_all_to_tracker_time = start_add_all_to_filter.elapsed();

    stats.total_time = start_f.elapsed();

    info!("lister thread returning after {:?} secs and listing {} files and local stat'ings of {}", start_f.elapsed(), stats.paths_listed, stats.paths_stat_ed);
    Ok(stats)
}

fn ticker(interval: Duration) {
    loop {
        sleep(interval);
        let xfer = STATS.xfer_count.fetch_add(0, Ordering::Relaxed);
        let dirs = STATS.dirs_check.fetch_add(0, Ordering::Relaxed);
        let path_ck = STATS.path_check.fetch_add(0, Ordering::Relaxed);
        let st_ck = STATS.stat_check.fetch_add(0, Ordering::Relaxed);
        let nev = STATS.never2xfer.fetch_add(0, Ordering::Relaxed);
        let yo = STATS.too_young.fetch_add(0, Ordering::Relaxed);
        debug!("xfer: {}  dir_entry: {}  paths: {}  stats: {}  never2xfer: {}  tooyoung: {}", xfer, dirs, path_ck, st_ck, nev, yo);
    }
}