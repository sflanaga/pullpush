#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

mod cli;
mod track;


use log::{info, debug, warn, error, trace};

use crate::cli::Cli;
use structopt::StructOpt;
use url::Url;
use anyhow::{anyhow, Context};
use std::path::{PathBuf, Path};
use std::net::TcpStream;
use ssh2::{Session, Sftp};
use std::time::{Duration, Instant};
use std::collections::{BTreeSet, BTreeMap};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::fmt::Display;

type Result<T> = anyhow::Result<T, anyhow::Error>;

use track::{TrackDelta, Tracker};
use std::ffi::{OsStr, OsString};

fn main() {
    if let Err(err) = run() {
        error!("{:?}", &err);
        std::process::exit(1);
    }
}

fn check_url(url: &Url) -> Result<()> {
    if url.port().is_none() { Err(anyhow!("Url MUST set port explicitly: {}", &url))? }
    if url.username().len() == 0 { Err(anyhow!("Url MUST set username explicitly: {}", &url))? }

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


fn xfer_file(path: &PathBuf, src: &Sftp, dst: &Sftp, dst_url: &Url) -> Result<()> {
    let mut dstPath = PathBuf::from(dst_url.path());
    let mut tmpPath = PathBuf::from(dst_url.path());
    let name = path.file_name().unwrap().to_str().unwrap();
    let tmpname = format!(".tmp{}", name);
    dstPath.push(&name[..]);
    tmpPath.push(&tmpname[..]);
    match dst.stat(&dstPath) {
        Err(e) => warn!("continue with error during stat of dest remote \"{}\", {}", &dstPath.display(), e),
        Ok(_) => {
            warn!("file: \"{}\" already at {}", &path.file_name().unwrap().to_string_lossy(), &dst_url);
            return Ok(());
        }
    }
    let mut f_in = BufReader::new(src.open(&path)?);
    let mut f_out = BufWriter::new(dst.create(&tmpPath)?);
    std::io::copy(&mut f_in, &mut f_out)?;
    dst.rename(&tmpPath, &dstPath, None).map_err(|e| error!("Cannot rename remote tmp to final: \"{}\" to \"{}\"", &tmpPath.display(), &dstPath.display()));
    info!("xferred: \"{}\" to {} \"{}\"", path.display(), &dst_url, &path.file_name().unwrap().to_string_lossy());
    Ok(())
}

fn run() -> Result<()> {
    println!("Hello, world!");

    stderrlog::new()
        .module(module_path!())
        .quiet(false)
        .verbosity(4)
        .timestamp(stderrlog::Timestamp::Millisecond) // opt.ts.unwrap_or(stderrlog::Timestamp::Off))
        .init()
        .unwrap();

    let cli = {
        let cli = Cli::from_args();
        check_url(&cli.src_url)?;
        check_url(&cli.dst_url)?;
        cli
    };
    info!("creating sftp connections");
    let src = create_sftp(&cli.src_url, &cli.src_pk, cli.timeout)?;
    let dst = create_sftp(&cli.dst_url, &cli.dst_pk, cli.timeout)?;

    let mut tracker = Tracker::new(&cli.track)?;

    debug!("listing remote");
    let mut count = 0;
    for (path, filestat) in src.readdir(&PathBuf::from(cli.src_url.path()))? {
        count += 1;
        if filestat.is_file() {
            trace!("considering file: {}", &path.display());
            let s = path.file_name()
                .ok_or(anyhow!("Cannot map path to a filename - weird \"{}\"", &path.display()))?.to_str().unwrap();
            if cli.re.is_match(s) {
                let xfer = match tracker.check(&path, &filestat)? {
                    TrackDelta::None => {
                        trace!("file \"{}\" not in tracker - xfer", &path.display());
                        true
                    },
                    TrackDelta::LastModChange => {
                        trace!("file \"{}\" last mod time has changed - xfer", &path.display());
                        true
                    },
                    TrackDelta::SizeChange => {
                        trace!("file \"{}\" size has changed - xfer", &path.display());
                        true
                    },
                    TrackDelta::Equal => {
                        trace!("file \"{}\" has already transferred according to tracking set", &path.display());
                        false
                    },
                };
                if xfer {
                    xfer_file(&path, &src, &dst, &cli.dst_url)?;
                    tracker.xferred(&path, &filestat)?;
                }
            } else {
                trace!("file \"{}\" does not match RE", s);
            }
        } else if filestat.is_dir() {
            trace!("dir: {}", &path.display());
        } else {
            let ft = &filestat.size.unwrap();
            trace!("not a dir or file: {}  size: {}", &path.display(), *ft);
        }
    }
    debug!("listed {} entries", count);
    std::process::exit(1);
    tracker.commit()?;
    //write_set(&cli.track, set).context("Failed to write track file")?;

    Ok(())
}
