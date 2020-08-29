#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unreachable_code)]


mod util;

use std::path::PathBuf;
use structopt::StructOpt;
use std::time::Instant;
use std::ops::Add;
use std::time::Duration;
use std::fs::{Metadata, DirEntry};
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use tokio::prelude::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering, AtomicU64};
use log::{debug, error, info, log, Record, trace, warn};
use lazy_static::lazy_static;

type Result<T> = std::result::Result<T, anyhow::Error>;

const ITR_COUNT: AtomicU64 = AtomicU64::new(0);

lazy_static! {
    pub static ref BUILD_INFO: String  = format!("ver: {}  rev: {}  date: {}", env!("CARGO_PKG_VERSION"), env!("VERGEN_SHA_SHORT"), env!("VERGEN_BUILD_DATE"));
}

#[derive(StructOpt, Debug, Clone)]
#[structopt(
version = BUILD_INFO.as_str(), rename_all = "kebab-case",
global_settings(& [
structopt::clap::AppSettings::ColoredHelp,
structopt::clap::AppSettings::UnifiedHelpMessage
]),
)]
/// test how fast you can list a directory and speed per entry
pub struct Cli {
    #[structopt(long, default_value("100000"))]
    /// how much to pre-allocated in time tracking vector
    pub vec_pre_alloc_size: usize,

    #[structopt()]
    /// path to the directory to read
    pub path: PathBuf,

    #[structopt(short, default_value("100"))]
    /// limit the detailed output
    pub limit_detailed_output: usize,

    #[structopt(short, long)]
    /// get metadata with each dir entry
    pub turn_on_meta_data: bool,

    #[structopt(short, long)]
    /// join and normalize the path for each dir entry
    pub canonicalize: bool,
}

#[derive(Debug)]
struct Job {
    pub path: PathBuf,
    pub md: Metadata,
    pub full: PathBuf,
}

fn main() -> Result<()> {
    let cli: Arc<Cli> = Arc::new(Cli::from_args());
    crate::util::init_log();

    println!("args: {:?}", &cli);

    //let mut times = Vec::with_capacity(cli.vec_pre_alloc_size);

    //let mut res = vec![];

    let start_t = Instant::now();

    let mut path_len = 0u64;
    let mut sum_size = 0u64;
    let mut last = Instant::now();

    let start_f = Instant::now();

    let cc = cli.clone();
    let x = futures::executor::block_on(actual(cc))?;

    let et = start_f.elapsed().as_secs_f64();
    let iters = ITR_COUNT.fetch_add(0, Ordering::Relaxed);
    let rate = iters as f64 / et;

    error!("time: {:?}  iter: {} GI  {} G-ops/s", start_f.elapsed(), ITR_COUNT.fetch_add(0, Ordering::Relaxed) / 1_000_000_000, rate / 1_000_000_000.0 / 16.0);



    Ok(())
}


async fn actual(cli: Arc<Cli>) -> Result<()> {
    let mut thread_count = Arc::new(AtomicUsize::new(0));

    let mut tc_1 = thread_count.clone();
    let mut tc_2 = thread_count.clone();

    let rt = tokio::runtime::Builder::new()
        .threaded_scheduler()
        .thread_stack_size(32 * 1024)
        .on_thread_start(move || {
            let c = tc_1.fetch_add(1, Ordering::Relaxed);
            debug!("THREAD START {} {}", c, std::thread::current().name().unwrap())
        })
        .on_thread_stop(move || {
            let c = tc_2.fetch_sub(1, Ordering::Relaxed);
            debug!("THREAD STOP {}", c)
        })
        .build().unwrap();


    info!("Hello, world!");
    let mut v = FuturesUnordered::new();
    for d in std::fs::read_dir(&cli.path)? {
        let cc = cli.clone();
        v.push(rt.spawn(eval_dir_entry(cc, d)));
    }


    info!("setup done");


    let mut count = 0;
    let mut size = 0u64;
    while let Some(x) = v.next().await {
        let goodr = x?;
        match goodr {
            None => (),
            Some(j) => {
                if count < cli.limit_detailed_output {
                    info!("{:?}", j)
                }
                count += 1;
                size += j.md.len();
            },
        }
    }

    info!("read {} entries  size: {:.3}GB", count, size as f64/((1<<30) as f64));
    //std::thread::sleep(Duration::from_secs(10));
    //Err(anyhow!("some made up error"))
    Ok(())
}
async fn eval_dir_entry(cli: Arc<Cli>, d: std::result::Result<DirEntry, std::io::Error>) -> Option<Job> {
    let mut size = 0u64;
    match d {
        Err(e) => error!("error getting d entry: {}", e),
        Ok(d) => {
            match d.metadata() {
                Err(e) => error!("error getting metadata: for {} {}", d.path().display(), e),
                Ok(md) => {
                    size = md.len();
                    match cli.path.join(d.path()).canonicalize() {
                        Err(e) => error!("cannot canonicalize path: {} {}", d.path().display(), e),
                        Ok(fullpath) => {
                            let name = d.path();
                            return Some(Job{
                                path: name,
                                md,
                                full: fullpath,
                            })
                        }
                    }
                },
            }
        }
    }
    None
}
