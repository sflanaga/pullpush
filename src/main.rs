#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

mod cli;

use log::{info, debug, warn, error, trace};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

use crate::cli::Cli;
use structopt::StructOpt;
use url::Url;
use anyhow::anyhow;
use std::path::{PathBuf, Path};
use std::net::TcpStream;
use ssh2::{Session, Sftp};
use std::time::Duration;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use ssh2::Error;

fn main() {
    if let Err(err) = run() {
        error!("error: {}", &err);
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
    let tcp = TcpStream::connect_timeout(&soc, timeout)?;
    let mut sess = Session::new().unwrap();
    sess.set_tcp_stream(tcp);
    sess.handshake().unwrap();
    sess.userauth_pubkey_file(&url.username(), None,
                              &pk, None)?;

    let sftp = sess.sftp()?;
    if sftp.stat(&*PathBuf::from(&url.path().to_string())).is_err() {
        Err(anyhow!("Cannot find remote path of url: {}", &url))?;
    };

    Ok(sftp)
}

fn read_lines<P>(filename: P) -> Result<std::io::Lines<std::io::BufReader<File>>>
    where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(std::io::BufReader::new(file).lines())
}

fn write_set(path: &PathBuf, set: BTreeSet<PathBuf>) -> Result<()> {
    let mut tmp_file = PathBuf::from(path.parent().unwrap());
    let final_file = path.clone();
    tmp_file.push(&format!(".tmp_{}", path.file_name().unwrap().to_string_lossy()));
    let mut f = std::fs::File::create(&tmp_file)?;
    let mut f = BufWriter::new(f);
    for p in set {
        write!(f, "{}\n", &p.display());
    }
    drop(f);
    info!("rename {:?} to {:?}", &tmp_file, &final_file);
    std::fs::rename(&tmp_file, final_file)?;

    Ok(())
}

fn read_into_set(path: &PathBuf) -> Result<BTreeSet<PathBuf>> {
    let mut set = BTreeSet::new();
    let lines = read_lines(&path);
    match lines {
        Ok(lines) => {
            for l in lines {
                let l = l?;
                set.insert(PathBuf::from(l));
            }
        }
        Err(e) => warn!("continuing but with warning opening track file \"{}\", {}", &path.display(), e),
    }
    Ok(set)
}

fn xfer_file(path: &PathBuf, src: &Sftp, dst: &Sftp, dst_url: &Url) -> Result<()> {
    let mut dstPath = PathBuf::from(dst_url.path());
    dstPath.push(path.file_name().unwrap());
    match dst.stat(&dstPath) {
        Err(e) => warn!("continue with error during stat of dest remote \"{}\", {}", &dstPath.display(), e),
        Ok(_) => {
            warn!("already xferred file: \"{}\" to {}", &path.file_name().unwrap().to_string_lossy(), &dst_url);
            return Ok(());
        }
    }
    let mut f_in = BufReader::new(src.open(&path)?);
    let mut f_out = BufWriter::new(dst.create(&dstPath)?);
    std::io::copy(&mut f_in, &mut f_out)?;
    info!("xferred: \"{}\" to {} \"{}\"", path.display(), &dst_url, &path.file_name().unwrap().to_string_lossy());
    Ok(())
}

fn run() -> Result<()> {
    println!("Hello, world!");

    stderrlog::new()
        .module(module_path!())
        .quiet(false)
        .verbosity(3)
        .timestamp(stderrlog::Timestamp::Millisecond) // opt.ts.unwrap_or(stderrlog::Timestamp::Off))
        .init()
        .unwrap();

    let cli = {
        let mut cli = Cli::from_args();
        check_url(&cli.src_url)?;
        check_url(&cli.dst_url)?;
        cli
    };
    info!("creating sftp connections");
    let src = create_sftp(&cli.src_url, &cli.src_pk, cli.timeout)?;
    let dst = create_sftp(&cli.dst_url, &cli.dst_pk, cli.timeout)?;

    let mut set = read_into_set(&cli.track)?;

    for f in src.readdir(&PathBuf::from(cli.src_url.path()))? {
        if f.0.is_file() {
            if cli.re.is_match(f.0.as_path().file_name().unwrap().to_str().unwrap()) {
                if !set.contains(f.0.as_path()) {
                    xfer_file(&f.0, &src, &dst, &cli.dst_url)?;
                    set.insert(f.0);
                }
            }
        }
    }
    write_set(&cli.track, set)?;

    Ok(())
}
