#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unreachable_code)]

use anyhow::{anyhow, Context};
use log::{debug, error, info, Record, trace, warn};
mod track;
mod vfs;
use track::Tracker;
use std::time::{Duration, Instant};
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use lazy_static::lazy_static;
type Result<T> = anyhow::Result<T, anyhow::Error>;
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
}
fn main() {
    match run() {
        Err(e) => println!("top level error: {:#?}", e),
        _ => (),
    }
    return ()
}

fn run() -> Result<()>{
    use simplelog::*;
    CombinedLogger::init(
        vec![
            TermLogger::new(LevelFilter::Debug, Config::default(), TerminalMode::Mixed)
            ]
    );

    let tot_cpu = cpu_time::ProcessTime::now();
    for _ in 0..8 {
        let start_f = Instant::now();
        let start_cpu = cpu_time::ProcessTime::now();
        let path = PathBuf::from("testit.track");
        let dur = Duration::from_secs(3600 * 24 * 180);
        let mut track = Tracker::new(&path, dur)?;
        println!("read {} entries in {:?}  cpu: {:?}", track.num_entries(), start_f.elapsed(), start_cpu.elapsed());

    }


    println!("total cpu: {:?}", tot_cpu.elapsed());

    Ok(())

}


