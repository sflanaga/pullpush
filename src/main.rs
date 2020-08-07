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
use ssh2::{Session, Sftp};
use structopt::StructOpt;
use url::Url;

use track::{TrackDelta, Tracker};

use crate::cli::Cli;

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

fn create_sftp(url: &Url, pk: &PathBuf, timeout: Duration) -> Result<Sftp> {
    let soc = url.socket_addrs(|| Some(22))?[0];
    let tcp = TcpStream::connect_timeout(&soc, timeout).with_context(|| format!("Tcp connection to url: {} failed", &url))?;
    let mut sess = Session::new().unwrap();
    sess.set_tcp_stream(tcp);
    sess.handshake().unwrap();
    sess.userauth_pubkey_file(&url.username(), None,
                              &pk, None).context("Unable to setup user with private key")?;

    let sftp = sess.sftp().context("Unable to setup sftp channel on this session")?;
    sftp.lstat(&*PathBuf::from(&url.path().to_string())).with_context(|| format!("Cannot stat check remote path of \"{}\"", url))?;

    Ok(sftp)
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
    let cli = {
        let mut cli = Cli::from_args();
        check_url(&cli.src_url)?;
        check_url(&cli.dst_url)?;
        if cli.verbosity == 0 {
            cli.verbosity = 2;
        }
        cli
    };
    eprintln!("verbose: {}", cli.verbosity);
    stderrlog::new()
        .module(module_path!())
        .quiet(false)
        .verbosity(cli.verbosity)
        .timestamp(stderrlog::Timestamp::Millisecond) // opt.ts.unwrap_or(stderrlog::Timestamp::Off))
        .init()
        .unwrap();

    info!("creating sftp connections");
    let src = create_sftp(&cli.src_url, &cli.src_pk, cli.timeout)?;
    let dst = create_sftp(&cli.dst_url, &cli.dst_pk, cli.timeout)?;

    let mut tracker = Tracker::new(&cli.track, cli.max_track_age)?;

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
                    let xfer = match tracker.check(&path, &filestat)? {
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
                        xfer_file(&path, &src, &dst, &cli.dst_url, cli.copy_buffer_size)?;
                        tracker.xferred(&path, &filestat)?;
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
    debug!("listed {} entries", count);
    tracker.commit()?;

    Ok(())
}
