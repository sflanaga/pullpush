#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use std::io::{BufReader, BufWriter};
use std::io::Write;
use std::net::TcpStream;
use std::ops::Add;
use std::path::{PathBuf};
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

mod cli;
mod track;
mod copier;


type Result<T> = anyhow::Result<T, anyhow::Error>;

fn main() {
    if let Err(err) = run() {
        error!("Error: {} / {:?} / {:#?}", &err, &err, &err);
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

    info!("creating sftp connections");
    let (src_sess, src) = create_sftp(&cli.src_url, &cli.src_pk, cli.timeout)?;
    let (dst_sess, _dst) = create_sftp(&cli.dst_url, &cli.dst_pk, cli.timeout)?;

    let tracker = Arc::new(RwLock::new(Tracker::new(&cli.track, cli.max_track_age)?));

    let (send, recv) = crossbeam_channel::unbounded();

    let mut xfer_threads = vec![];
    for i in 0..cli.threads {
        let recv_c = recv.clone();
        let cli_c = cli.clone();
        let mut tracker_c = tracker.clone();

        let (src, dst) = if cli.reuse_sessions {
            let src = src_sess.sftp().with_context(|| format!("Unable to create the next channel at {} to src url - usually limited to 10 so set --threads to 8 maybe", i + 2))?;
            let dst = dst_sess.sftp().with_context(|| format!("Unable to create the next channel at {} to dst url - usually limited to 10 so set --threads to 8 maybe", i + 2))?;
            (Some(src), Some(dst))
        } else {
            warn!("Creating new session: {}", i + 1);
            // this forces the transfer thread to create it's own session
            (None, None)
        };
        let h = Builder::new().name(format!("{}:{}", "xfer", i)).spawn(move || xferring(&recv_c, &cli_c, src, dst, &mut tracker_c)).unwrap();
        xfer_threads.push(h);
    }

    let start = Instant::now();

    debug!("listing remote");
    let mut count_files_listed = 0;
    let now = SystemTime::now();

    let h_lister_thread = {
        let (cli_c, tracker_c, send_c) = (cli.clone(), tracker.clone(), send.clone());
        Builder::new().name("lister".to_string()).spawn(move || lister_thread(&cli_c, src, &tracker_c, &send_c)).unwrap()
    };

    info!("lister completed listing: {} entries", h_lister_thread.join().unwrap()?);

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

    Ok(())
}


fn check_url(url: &Url) -> Result<()> {
    if url.port().is_none() { return Err(anyhow!("Url MUST set port explicitly: {}", &url)); }
    if url.username().len() == 0 { return Err(anyhow!("Url MUST set username explicitly: {}", &url)); }

    Ok(())
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


fn xfer_file(cli_c: &Arc<Cli>, path: &PathBuf, filestat: &FileStat, perm: i32, src: &Sftp, dst: &Sftp, dst_url: &Url, copy_buffer_size: usize) -> Result<(u64, u64)> {
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
            warn!("file: \"{}\" already at {}", &path.file_name().unwrap().to_string_lossy(), &dst_url);
            return Ok((0, 0));
        }
    }
    let mut f_in = Arc::new(Mutex::new(BufReader::with_capacity(copy_buffer_size, src.open(&path)?)));
    let mut f_out = Arc::new(Mutex::new(BufWriter::with_capacity(copy_buffer_size,
                                                                 dst.open_mode(&tmp_path,
                                                                               OpenFlags::WRITE | OpenFlags::TRUNCATE, perm, OpenType::File)?)));
    let size = copier::copier(&mut f_in, &mut f_out, cli_c.copy_buffer_size, cli_c.buffer_ring_size)?;
    //let size = std::io::copy(&mut f_in, &mut f_out)?;

    match dst.rename(&tmp_path, &dst_path, None) {
        Err(e) => error!("Cannot rename remote tmp to final: \"{}\" to \"{}\" due to {:?}", &tmp_path.display(), &dst_path.display(), e),
        Ok(()) => {
            let t = timer.elapsed().as_secs_f64();
            let r = (size as f64) / t;
            info!("xferred: \"{}\" to {} \"{}\" size: {}  rate: {:.3}MB/s  time: {:.3} secs", path.display(), &dst_url, &path.file_name().unwrap().to_string_lossy(),
                  size, r / (1024f64 * 1024f64), t)
        }
    }

    let mut newstats = FileStat { perm: Some(perm as u32), mtime: None, size: None, atime: None, gid: None, uid: None };
    dst.setstat(&dst_path, newstats)?;

    /* for now setting time remotely does not seem to work - here's what I tried:

    let mut newstats = FileStat {perm: None, mtime: Some(filestat.mtime.unwrap()), size: None, atime: None, gid: None, uid: None};
    dst.setstat(&dst_path, newstats)?;

     */

    Ok((1, size as u64))
}



fn xferring(recv_c: &Receiver<Option<(PathBuf, FileStat)>>, cli_c: &Arc<Cli>, src: Option<Sftp>, dst: Option<Sftp>, tracker: &mut Arc<RwLock<Tracker>>) -> (u64, u64) {
    match xferring_inn(recv_c, cli_c, src, dst, tracker) {
        Err(e) => {
            error!("sending thread died: {:#?} - maybe the others will get it down this round", e);
            (0, 0)
        }
        Ok(x) => x,
    }
}

fn xferring_inn(recv_c: &Receiver<Option<(PathBuf, FileStat)>>, cli_c: &Arc<Cli>, src: Option<Sftp>, dst: Option<Sftp>, tracker: &mut Arc<RwLock<Tracker>>) -> Result<(u64, u64)> {
    let (l_src, l_dst) = if src.is_some() {
        (src.unwrap(), dst.unwrap())
    } else {
        let src = create_sftp(&cli_c.src_url, &cli_c.src_pk, cli_c.timeout)?;
        let dst = create_sftp(&cli_c.dst_url, &cli_c.dst_pk, cli_c.timeout)?;
        info!("creating src/dst sessions");

        (src.1, dst.1)
    };

    let mut count = 0u64;
    let mut size = 0u64;
    loop {
        let p = recv_c.recv()?;
        match p {
            None => return Ok((count, size)),
            Some((path, filestat)) => {
                let (c, s) = xfer_file(&cli_c, &path, &filestat, cli_c.dst_perm, &l_src, &l_dst, &cli_c.dst_url, cli_c.copy_buffer_size)?;
                size += s;
                count += c;
                tracker.write().unwrap().xferred(&path, &filestat)?;
            }
        }
    }
    // Ok((count, size))
}

fn get_file_age(path: &PathBuf, filestat: &FileStat) -> Duration {
    let now = SystemTime::now();
    let ft = SystemTime::UNIX_EPOCH.add(Duration::from_secs(filestat.mtime.unwrap()));

    match now.duration_since(ft) {
        Err(e) => {
            warn!("got \"future\" time for path \"{}\", so assuming 0 age.  {:#?}", path.display(), &e);
            Duration::from_secs(0)
        }, // pretend its now
        Ok(dur) => dur,
    }
}

fn lister_thread(cli: &Arc<Cli>, src: Sftp, tracker: &Arc<RwLock<Tracker>>, send: &Sender<Option<(PathBuf, FileStat)>>) -> Result<u64> {
    let mut count_files_listed = 0u64;
    for (path, filestat) in src.readdir(&PathBuf::from(cli.src_url.path()))? {
        count_files_listed += 1;
        if filestat.is_file() {
            trace!("considering file: {}", &path.display());
            let s = path.file_name()
                .ok_or(anyhow!("Cannot map path to a filename - weird \"{}\"", &path.display()))?.to_str().unwrap();
            let age = get_file_age(&path, &filestat);
            if age > cli.max_age {
                trace!("file \"{}\" to old at {:?}", &path.display(), age);
            } else if age < cli.min_age {
                trace!("file \"{}\" to new at {:?}", &path.display(), age);
            } else if s.starts_with('.') && !cli.include_dot_files {
                trace!("file \"{}\" excluded as a dot file or hidden", &path.display());
            } else {
                trace!("file \"{}\" has good age at {:?}", &path.display(), age);
                if cli.re.is_match(s) {
                    let xfer = match tracker.write().unwrap().check(&path, &filestat)? {
                        TrackDelta::None => {
                            trace!("transferring file \"{}\" not in tracker", &path.display());
                            true
                        }
                        TrackDelta::LastModChange => {
                            trace!("transferring file \"{}\" last mod time has changed", &path.display());
                            true
                        }
                        TrackDelta::SizeChange => {
                            trace!("transferring file \"{}\" size has changed", &path.display());
                            true
                        }
                        TrackDelta::Equal => {
                            trace!("skipping file \"{}\" has already transferred according to tracking set", &path.display());
                            false
                        }
                    };
                    if xfer {
                        if !cli.dry_run {
                            send.send(Some((path.clone(), filestat.clone())))?;
                        } else {
                            info!("would have xferred file: \"{}\"", &path.display());
                            tracker.write().unwrap().xferred(&path, &filestat)?;
                        }
                    }
                } else {
                    trace!("file \"{}\" does not match RE", s);
                }
            }
        } else if filestat.is_dir() {
            trace!("dir: {}", &path.display());
        } else {
            let ft = &filestat.size.unwrap();
            trace!("not a dir or file: {}  size: {}", &path.display(), *ft);
        }
    }
    Ok(count_files_listed)
}