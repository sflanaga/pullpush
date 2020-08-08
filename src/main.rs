// #![allow(dead_code)]
// #![allow(unused_imports)]
// #![allow(unused_variables)]
//
use std::io::{BufReader, BufWriter};
use std::net::TcpStream;
use std::ops::Add;
use std::path::{PathBuf};
use std::time::{Duration, SystemTime, Instant};

use anyhow::{anyhow, Context};
use log::{debug, error, info, trace, warn};
use ssh2::{Session, Sftp, FileStat};
use structopt::StructOpt;
use url::Url;
use crossbeam_channel::{Receiver, Sender};
use track::{TrackDelta, Tracker};

use crate::cli::Cli;
use std::sync::{Arc, RwLock};

mod cli;
mod track;


type Result<T> = anyhow::Result<T, anyhow::Error>;

fn main() {
    if let Err(err) = run() {
        error!("{:?}", &err);
        std::process::exit(1);
    }
}

fn check_url(url: &Url) -> Result<()> {
    if url.port().is_none() { return Err(anyhow!("Url MUST set port explicitly: {}", &url)); }
    if url.username().len() == 0 { return Err(anyhow!("Url MUST set username explicitly: {}", &url)); }

    Ok(())
}

fn create_sftp(url: &Url, pk: &PathBuf, timeout: Duration) -> Result<(Session,Sftp)> {
    let soc = url.socket_addrs(|| Some(22))?[0];
    let tcp = TcpStream::connect_timeout(&soc, timeout).with_context(|| format!("Tcp connection to url: {} failed", &url))?;
    let mut sess = Session::new().unwrap();
    sess.set_tcp_stream(tcp);
    sess.handshake()?;
    sess.userauth_pubkey_file(&url.username(), None,
                              &pk, None).context("Unable to setup user with private key")?;

    let sftp = sess.sftp().context("Unable to setup sftp channel on this session")?;
    sftp.lstat(&*PathBuf::from(&url.path().to_string())).with_context(|| format!("Cannot stat check remote path of \"{}\"", url))?;

    Ok((sess, sftp))
}


fn xfer_file(path: &PathBuf, src: &Sftp, dst: &Sftp, dst_url: &Url, copy_buffer_size: usize) -> Result<()> {
    let mut dst_path = PathBuf::from(dst_url.path());
    let mut tmp_path = PathBuf::from(dst_url.path());
    let name = path.file_name().unwrap().to_str().unwrap();
    let tmpname = format!(".tmp{}", name);
    dst_path.push(&name[..]);
    tmp_path.push(&tmpname[..]);
    let timer = Instant::now();
    match dst.stat(&dst_path) {
        Err(e) => (), // silencing useless info... for now warn!("continue with error during stat of dest remote \"{}\", {}", &dst_path.display(), e),
        Ok(_) => {
                warn!("file: \"{}\" already at {}", &path.file_name().unwrap().to_string_lossy(), &dst_url);
            return Ok(());
        }
    }
    let mut f_in = BufReader::with_capacity(copy_buffer_size, src.open(&path)?);
    let mut f_out = BufWriter::with_capacity(copy_buffer_size, dst.create(&tmp_path)?);
    let size = std::io::copy(&mut f_in, &mut f_out)?;
    match dst.rename(&tmp_path, &dst_path, None) {
        Err(e) => error!("Cannot rename remote tmp to final: \"{}\" to \"{}\" due to {}", &tmp_path.display(), &dst_path.display(), e),
        Ok(()) => {
            let t = timer.elapsed().as_secs_f64();
            let r = (size as f64) / t;
            info!("xferred: \"{}\" to {} \"{}\" size: {}  rate: {:.3}MB/s  time: {:.3} secs", path.display(), &dst_url, &path.file_name().unwrap().to_string_lossy(),
                  size, r / (1024f64 * 1024f64), t)
        }
    }

    Ok(())
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
    eprintln!("verbose: {}", cli.verbosity);
    stderrlog::new()
        .module(module_path!())
        .quiet(false)
        .verbosity(cli.verbosity)
        .timestamp(stderrlog::Timestamp::Millisecond) // opt.ts.unwrap_or(stderrlog::Timestamp::Off))
        .init()
        .unwrap();

    info!("creating sftp connections");
    let (src_sess, src) = create_sftp(&cli.src_url, &cli.src_pk, cli.timeout)?;
    let (dst_sess, dst) = create_sftp(&cli.dst_url, &cli.dst_pk, cli.timeout)?;

    let mut tracker = Arc::new(RwLock::new(Tracker::new(&cli.track, cli.max_track_age)?));

    let (send,recv) = crossbeam_channel::unbounded();

    let mut hv = vec![];
    for i in 0..cli.threads {
        let recv_c = recv.clone();
        let cli_c = cli.clone();
        let mut tracker_c = tracker.clone();

        let (src,dst) = if cli.session_per_thread {
            warn!("Creating new session: {}", i+1);
            let (_, src) = create_sftp(&cli.src_url, &cli.src_pk, cli.timeout)?;
            let (_, dst) = create_sftp(&cli.dst_url, &cli.dst_pk, cli.timeout)?;
            (src,dst)
        } else {
            let src = src_sess.sftp().with_context(||format!("Unable to create the next channel at {} to src url - usually limited to 10 so set --threads to 8 maybe", i+2))?;
            let dst = dst_sess.sftp().with_context(||format!("Unable to create the next channel at {} to dst url - usually limited to 10 so set --threads to 8 maybe", i+2))?;
            (src,dst)
        };
        let h = std::thread::spawn(move || xferring(&recv_c, &cli_c, src, dst, &mut tracker_c));
        hv.push(h);
    }


    debug!("listing remote");
    let mut count = 0;
    let now = SystemTime::now();
    for (path, filestat) in src.readdir(&PathBuf::from(cli.src_url.path()))? {
        count += 1;
        if filestat.is_file() {
            trace!("considering file: {}", &path.display());
            let s = path.file_name()
                .ok_or(anyhow!("Cannot map path to a filename - weird \"{}\"", &path.display()))?.to_str().unwrap();

            let ft = SystemTime::UNIX_EPOCH.add(Duration::from_secs(filestat.mtime.unwrap()));
            let age = now.duration_since(ft).with_context(|| format!("mtime calc: mtime: {}, now: {:?} ft: {:?}", filestat.mtime.unwrap(), now, ft))?;
            if age > cli.max_age {
                trace!("file \"{}\" to old at {:?}", &path.display(), age);
            } else if age < cli.min_age {
                trace!("file \"{}\" to new at {:?}", &path.display(), age);
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

    for _ in &hv {
        send.send(None)?;
    }
    for h in hv {
        h.join().unwrap();
    }

    debug!("listed {} entries", count);
    tracker.write().unwrap().commit()?;

    Ok(())
}


fn xferring(recv_c: &Receiver<Option<(PathBuf, FileStat)>>, cli_c: &Arc<Cli>, src: Sftp, dst: Sftp, tracker: &mut Arc<RwLock<Tracker>>) {
    match xferring_inn(recv_c, cli_c, src, dst, tracker) {
        Err(e) => error!("sending thread died: {} - maybe the others will get it down this round", e),
        Ok(_) => (),
    }
}

fn xferring_inn(recv_c: &Receiver<Option<(PathBuf, FileStat)>>, cli_c: &Arc<Cli>, src: Sftp, dst: Sftp, tracker: &mut Arc<RwLock<Tracker>>) -> Result<()> {
    // let src = create_sftp(&cli_c.src_url, &cli_c.src_pk, cli_c.timeout)?;
    // let dst = create_sftp(&cli_c.dst_url, &cli_c.dst_pk, cli_c.timeout)?;
    loop {
        let p = recv_c.recv()?;
        match p {
            None => return Ok(()),
            Some(p) => {
                xfer_file(&p.0, &src, &dst, &cli_c.dst_url, cli_c.copy_buffer_size)?;
                tracker.write().unwrap().xferred(&p.0, &p.1)?;
            }
        }
    }
    Ok(())
}