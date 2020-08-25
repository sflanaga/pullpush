#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use anyhow::{anyhow as ERR, Context};
mod vfs;

use vfs::Vfs;
use url::Url;
use std::time::Duration;
use std::path::PathBuf;
use ssh2::Sftp;
use std::io::Write;

type Result<T> = anyhow::Result<T, anyhow::Error>;

fn main() -> Result<()> {

    let u = Url::parse(" sftp://steve@127.0.0.1:22/home/steve/testsrc/")?;
    let u = Url::parse(" file:///Users/Steve/")?;
    let pk = PathBuf::from("C:\\Users\\Steve\\.ssh\\id_rsa");
    let to = Duration::from_secs(30);
    eprintln!("create vfs");
    let mut v = vfs::create_vfs(&u, None, &Some(pk), Some(to))?;
    eprintln!("clone base dir");
    let p = v.base_dir().clone();
    eprintln!("open dir {}", p.display());
    let mut od = v.open_dir(&p)?;

    eprintln!("listing");

    loop {
        match od.read_all_dir_entry(Some(|x|true), Some(|y| true)) {
            Err(e) => return Err(ERR!("could not readdir {}", e)),
            Ok(Some(path)) => eprintln!("path: {}", path.display()),
            Ok(None) => break,
        }
    }


    let mut rdr = v.open(PathBuf::from("/Users/Steve/stateit").as_ref())?;

    loop {
        let mut buf = vec![0u8; 1024];
        let len = rdr.read(&mut buf[..])?;
        if len == 0 {
            break;
        }
        std::io::stdout().lock().write(&buf[..len]);
    }

    let s = r"
    CHNAGED  this is the stuff
    we will write ...............
    ";

    let mut wtr = v.create(PathBuf::from("/Users/Steve/funfile").as_ref())?;
    wtr.write_all(s.as_bytes())?;

    eprintln!("DONE");



    Ok(())
}