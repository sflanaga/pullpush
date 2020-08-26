#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unreachable_code)]

use std::io::{BufReader, BufWriter};
use std::io::Write;
use std::net::TcpStream;
use std::ops::{Add, Sub};
use std::path::{PathBuf, Path};
use std::time::{Duration, SystemTime, Instant};

use anyhow::{anyhow, Context};
use log::{debug, error, info, trace, warn, Record};
use ssh2::{Session, Sftp, FileStat, OpenFlags, OpenType};
use structopt::StructOpt;
use url::Url;
use crossbeam_channel::{Receiver, Sender};
use track::{TrackDelta, Tracker};

use crate::cli::Cli;
use std::sync::{Arc, RwLock, Mutex};
use std::thread;
use chrono::Utc;
use env_logger::Env;
use env_logger::fmt::Color;

use log::Level;
use std::thread::Builder;
use libssh2_sys::LIBSSH2_ERROR_FILE;

mod cli;
mod track;
mod copier;
mod vfs;
mod fast_stat;

#[derive(Debug)]
pub struct Stats {
    pub xfer_count: AtomicUsize,
    pub path_check: AtomicUsize,
    pub stat_check: AtomicUsize,
    pub never2xfer: AtomicUsize,
    pub too_young: AtomicUsize,
}

use lazy_static::lazy_static;

lazy_static! {
    pub static ref STATS: Stats = Stats {
        xfer_count: AtomicUsize::new(0),
        path_check: AtomicUsize::new(0),
        stat_check: AtomicUsize::new(0),
        never2xfer: AtomicUsize::new(0),
        too_young: AtomicUsize::new(0),
    };
}

use vfs::{Vfs, create_vfs, VfsFile, FileStatus, FileType};
use std::sync::atomic::{AtomicUsize, Ordering};

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
        if cli.verbosity == 0 {
            cli.verbosity = 2;
        }
        if cli.overwrite_if_stats_change {
            return Err(anyhow!("overwrite_if_stats_change not supported yet"));
        }
        cli
    });

    let mut builder = env_logger::Builder::new();

    builder.format(|buf, record: &Record| {
        writeln!(buf, "{} [{:4}] [{}:{}] {:>5}: {} ", Utc::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                 thread::current().name().or(Some("unknown")).unwrap(),
                 record.file().unwrap(),
                 record.line().unwrap(),
                 record.level(),
                 record.args())
    });
    let log_level = match (cli.quiet, cli.verbosity) {
        (true, _) => log::LevelFilter::Off,
        (false, 0) => log::LevelFilter::Error,
        (false, 1) => log::LevelFilter::Warn,
        (false, 2) => log::LevelFilter::Info,
        (false, 3) => log::LevelFilter::Debug,
        (false, _) => log::LevelFilter::Trace,
    };
    builder.filter_level(log_level);

    builder.init();

    let mut src = vfs::create_vfs(&cli.src_url, cli.dst_perm, &cli.src_pk, Some(cli.timeout))?;
    // we do not use this dst but it is done to make sure the downstream can connect before too much machinery
    // get going.  Might be removed later.
    let mut dst = vfs::create_vfs(&cli.dst_url, cli.dst_perm, &cli.dst_pk, Some(cli.timeout))?;

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
    let mut count_files_listed = 0;
    let now = SystemTime::now();

    let h_lister_thread = {
        let (cli_c, tracker_c, send_c) = (cli.clone(), tracker.clone(), send.clone());
        trace!("starting lister thread");
        Builder::new().name("lister".to_string()).spawn(move || lister_thread(&cli_c, src, &tracker_c, &send_c)).context("lister thread start failed")?
    };
    trace!("lister started");

    info!("lister completed listing: {} entries in {:.3} seconds", h_lister_thread.join().unwrap()?, now.elapsed()?.as_secs_f64());

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
    info!("listed {} entries, trans: {} files  {:.3} MB in {:.3} secs", count_files_listed, count, mb, start.elapsed().as_secs_f64());
    tracker.write().unwrap().commit()?;

    info!("STATS: {:#?}", *STATS);

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

fn create_sftp(url: &Url, pk: &PathBuf, timeout: Duration) -> Result<(Session, Sftp)> {
    let soc = url.socket_addrs(|| Some(22))?[0];
    let tcp = TcpStream::connect_timeout(&soc, timeout).with_context(|| format!("Tcp connection to url: {} failed", &url))?;


    let mut sess = Session::new().unwrap();
    sess.set_tcp_stream(tcp);
    sess.handshake()?;
    sess.userauth_pubkey_file(&url.username(), None,
                              &pk, None).with_context(|| format!("Unable to setup user with private key: {} for url {}", pk.display(), &url))?;

    let sftp = sess.sftp().with_context(|| format!("Unable to create sftp session with private key: {} for url {}", pk.display(), &url))?;
    sftp.lstat(&*PathBuf::from(&url.path().to_string())).with_context(|| format!("Cannot stat check remote path of \"{}\"", url))?;

    Ok((sess, sftp))
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
    let src = vfs::create_vfs(&cli.src_url, cli.dst_perm, &cli.src_pk, Some(cli.timeout))?;
    let dst = vfs::create_vfs(&cli.dst_url, cli.dst_perm, &cli.dst_pk, Some(cli.timeout))?;

    let mut count = 0u64;
    let mut size = 0u64;
    loop {
        let p = recv_c.recv().context("receiving next entry in channel")?;
        match p {
            None => return Ok((count, size)),
            Some((path, filestat)) => {
                let (c, s) = xfer_file(&cli, &path, &filestat, &src, &dst, &cli.dst_url, cli.copy_buffer_size)?;
                STATS.xfer_count.fetch_add(1, Ordering::Relaxed);
                size += s;
                count += c;
                tracker.write().unwrap().xferred(&path, filestat)?;
            }
        }
    }
    // Ok((count, size))
}

fn xfer_file(cli_c: &Arc<Cli>, path: &PathBuf, filestat: &FileStatus, src: &Box<dyn Vfs + Send>, dst: &Box<dyn Vfs + Send>, dst_url: &Url, copy_buffer_size: usize) -> Result<(u64, u64)> {
    let mut dst_path = PathBuf::from(dst_url.path());
    let mut tmp_path = PathBuf::from(dst_url.path());
    let name = path.file_name().unwrap().to_str().unwrap();
    let tmpname = format!(".tmp{}", name);
    dst_path.push(&name[..]);
    tmp_path.push(&tmpname[..]);

    let timer = Instant::now();
    match dst.stat(&dst_path) {
        Err(_) => (), // silencing useless info... for now warn!("continue with error during stat of dest remote \"{}\", {}", &dst_path.display(), e),
        Ok(_) => {
            warn!("file: \"{}\" already at {} and recording it as xferred - no overwrite option yet", &path.file_name().unwrap().to_string_lossy(), &dst_url);
            return Ok((0, 0));
        }
    }

    // let size = if cli_c.threaded_copy {
    //     let mut f_in = Arc::new(Mutex::new(src.open(&path).context("open src file - tcopy")?));
    //     let mut f_out = Arc::new(Mutex::new(dst.create(&tmp_path).context("opening dst file - tcopy")?));
    //     copier::copier(cli_c.threaded_copy_fill_buffer, &mut f_in, &mut f_out, cli_c.copy_buffer_size, cli_c.buffer_ring_size)?
    // } else {
    let mut f_in = BufReader::with_capacity(copy_buffer_size, src.open(&path).with_context(|| format!("opening src file direct: {}", path.display()))?);
    let mut f_out = BufWriter::with_capacity(copy_buffer_size,
                                             dst.create(&tmp_path).context("opening dst file direct")?);
    let size = std::io::copy(&mut f_in, &mut f_out)? as usize;
    // };

    match dst.rename(&tmp_path, &dst_path) {
        Err(e) => error!("Cannot rename remote tmp to final: \"{}\" to \"{}\" due to {:?}", &tmp_path.display(), &dst_path.display(), e),
        Ok(()) => {
            let t = timer.elapsed().as_secs_f64();
            let r = (size as f64) / t;
            info!("xferred: \"{}\" to {} \"{}\" size: {}  rate: {:.3}MB/s  time: {:.3} secs", path.display(), &dst_url, &path.file_name().unwrap().to_string_lossy(),
                  size, r / (1024f64 * 1024f64), t)
        }
    }

    dst.set_perm(&dst_path)?;

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


fn keep_path(cli: &Arc<Cli>, path: &PathBuf, tracker: &Arc<RwLock<Tracker>>) -> Result<bool> {
    STATS.path_check.fetch_add(1, Ordering::Relaxed);

    let s = match path.file_name() {
        None => {
            error!("Cannot map path to a filename - weird \"{}\"", &path.display());
            return Ok(false);
        }
        Some(s) => s.to_string_lossy(),
    };

    if !cli.re.is_match(&s.as_bytes())? {
        trace!("file \"{}\" does not match RE", s);
        return Ok(false);
    }

    if s.starts_with('.') && !cli.include_dot_files {
        trace!("file \"{}\" excluded as a dot file or hidden", &path.display());
        return Ok(false);
    }

    match tracker.read() {
        Err(e) => return Err(anyhow!("could not read lock tracker due to {}", &e)),
        Ok(l) => {
            if l.path_exists_in_tracker(&path) {
                trace!("file \"{}\" already in tracker", &path.display());
                return Ok(false);
            }
        }
    }

    Ok(true)
}

const FILE_TOO_OLD: u32 = 1;
const FILE_TOO_YOUNG: u32 = 2;
const FILE_NOT_A_FILE: u32 = 4;

fn keep_status(cli: &Arc<Cli>, path: &PathBuf, filestatus: FileStatus) -> Result<u32> {
    STATS.stat_check.fetch_add(1, Ordering::Relaxed);

    if filestatus.file_type == vfs::FileType::Regular {
        let age = get_file_age(&path, &filestatus);
        if age > cli.max_age {
            trace!("file \"{}\" too old at {:?}", &path.display(), age);
            return Ok(FILE_TOO_OLD);
        } else if age < cli.min_age {
            trace!("file \"{}\" too new at {:?}", &path.display(), age);
            return Ok(FILE_TOO_YOUNG);
        }
        Ok(0)
    } else {
        trace!("dir: {}", &path.display());
        return Ok(FILE_NOT_A_FILE);
    }
}

fn lister_thread(cli: &Arc<Cli>, mut src: Box<dyn Vfs + Send>, tracker: &Arc<RwLock<Tracker>>, send: &Sender<Option<(PathBuf, FileStatus)>>) -> Result<u64> {
    match inner_lister_thread(cli, src, tracker, send) {
        Err(e) => {
            error!("lister thread failed: {:?}", e);
            return Err(e);
        }
        Ok(c) => return Ok(c),
    }
}

fn inner_lister_thread(cli: &Arc<Cli>, mut src: Box<dyn Vfs + Send>, tracker: &Arc<RwLock<Tracker>>, send: &Sender<Option<(PathBuf, FileStatus)>>) -> Result<u64> {
    let start_f = Instant::now();
    let mut count_files_listed = 0u64;
    let mut count_files_stat_ed = 0u64;
    let dir_path = &PathBuf::from(cli.src_url.path());
    trace!("opening dir: {}", dir_path.display());
    let mut dir = src.open_dir(&dir_path).with_context(|| format!("open dir on base directory: {}", dir_path.display()))?;

    let mut list = dir.read_all_dir_entry().context("error on next_dir_entry")?;
    info!("file list {} in {:?}", list.len(), start_f.elapsed());

    let mut xfer_list = vec![];
    let mut with_stat_list = vec![];

    let has_stat = list.len() > 0 && list[0].1.is_some();

    let mut list = if !has_stat {
        let start_f = Instant::now();
        let mut path_checked_list = list.iter()
            .map(|(p, o)| (dir_path.join(&p), o))
            .filter(|(p, o)| {
                match keep_path(&cli, p, &tracker) {
                    Err(e) => {
                        error!("Unable to check path {} due to {}", p.display(), e);
                        false
                    }
                    Ok(keep) => keep,
                }
            }).map(|(p, o)| p).collect::<Vec<_>>();
        info!("check time: {:?}", start_f.elapsed());
        let start_f = Instant::now();
        let x = fast_stat::get_stats_fast(cli.local_file_stat_thread_pool_size, &mut path_checked_list).context("get fast stats failure")?;
        info!("fast stats time: {:?}", start_f.elapsed());
        x
    } else {
        list.iter().map(|(p,o)| (dir_path.join(p).clone(), o.unwrap().clone())).collect::<Vec<_>>()
    };

    info!("file passing check path {}", list.len());

    for (path, filestatus) in list.iter() {
        let k_s = keep_status(&cli, &path, *filestatus)?;
        count_files_stat_ed += 1;
        if k_s & FILE_NOT_A_FILE != 0 || k_s & FILE_TOO_OLD != 0 {
            // these file should never be transferred in the future
            STATS.never2xfer.fetch_add(1, Ordering::Relaxed);
            with_stat_list.push((path.clone(), filestatus));
        } else if k_s & FILE_TOO_YOUNG != 0 {
            STATS.too_young.fetch_add(1, Ordering::Relaxed);
            // do nothing but it will show up again and be old enough
            // and should be xferred
        } else {
            if !cli.dry_run {
                if cli.queue_as_found {
                    trace!("queueing file: {}", path.display());
                    send.send(Some((path.clone(), *filestatus)))?;
                } else {
                    xfer_list.push((path.clone(), filestatus.clone()));
                }
            } else {
                trace!("would have xferred file: {}", path.display());
            }
        }
    }
    if !cli.queue_as_found {
        trace!("queueing all files for xfer at once");
        let start_f = Instant::now();
        let count = xfer_list.len();
        loop {
            match xfer_list.pop() {
                None => break,
                Some(x) => {
                    trace!("queueing file: {}", x.0.display());
                    send.send(Some(x))?
                }
            }
        }
        info!("vec to queue {} in: {:?}", count, start_f.elapsed());
    }
    if cli.add_all_to_tracker {
        trace!("write just paths to track for later speeder listings");
        let count = with_stat_list.len();
        let start_f = Instant::now();
        loop {
            match with_stat_list.pop() {
                None => break,
                Some(x) => tracker.write().unwrap().insert_path_and_status(&x.0, *x.1)?,
            }
        }
        info!("vec of ignorable in future files {} to tracker in {:?}", count, start_f.elapsed());
    }


    info!("lister thread returning after {:?} secs and listing {} files and local stat'ings of {}", start_f.elapsed(), count_files_listed, count_files_stat_ed);
    Ok(count_files_listed)
}