#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unreachable_code)]


use std::path::PathBuf;
use structopt::StructOpt;
use std::time::Instant;
use std::ops::Add;
use std::time::Duration;
use std::fs::Metadata;
use rayon::prelude::*;

type Result<T> = std::result::Result<T, anyhow::Error>;

#[derive(StructOpt, Debug, Clone)]
#[structopt(
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
    let cli: Cli = Cli::from_args();
    println!("args: {:?}", &cli);

    //let mut times = Vec::with_capacity(cli.vec_pre_alloc_size);

    //let mut res = vec![];

    let start_t = Instant::now();

    let mut path_len = 0u64;
    let mut sum_size = 0u64;
    let mut last = Instant::now();

    let res = std::fs::read_dir(&cli.path)?.par_bridge().map(|p| {
        let p = p.unwrap();
        let p = p.path();
        let md: Metadata = p.metadata().unwrap();
        (p, md)
    }).into_par_iter().map(|(p,md)| {
        let fp = cli.path.join(&p).canonicalize().unwrap();
        Job {
            path: p.clone(),
            md: md.clone(),
            full: fp,
        }
    }).collect::<Vec<Job>>();


    println!("finished listing {} in {:?}", res.len(), start_t.elapsed());

    for j in res.iter().enumerate() {
        println!("{:?}", &j.1);
        if j.0 > cli.limit_detailed_output {
            break;
        }
    }


    // for file in std::fs::read_dir(&cli.path)? {
    //     let now = Instant::now();
    //     let file = file?;
    //     if cli.turn_on_meta_data {
    //         let md: Metadata = file.metadata()?;
    //         if md.is_file() {
    //             sum_size += md.len();
    //         }
    //     }
    //     if cli.canonicalize {
    //         let full_path = &cli.path.join(file.path()).canonicalize()?;
    //         path_len += full_path.components().count() as u64;
    //     } else {
    //         path_len += file.path().components().count() as u64;
    //     }
    //     times.push(now - last);
    //     last = Instant::now();
    // }
    //
    // println!("list {} entries in {:.9} secs  total size: {}", times.len(), start_t.elapsed().as_secs_f64(), sum_size);
    //
    // let mut min = Duration::from_secs(3600);
    // let mut max = Duration::from_secs(0);
    // let mut sum = Duration::from_secs(0);
    // let mut count = 0;
    // for t in &times {
    //     count += 1;
    //     sum = sum.add(*t);
    //     min = *t.min(&min);
    //     max = *t.max(&max);
    //     if count < cli.limit_detailed_output {
    //         println!("time: {:?}", t);
    //     }
    // }
    //
    // let mn = min.as_secs_f64();
    // let mx = max.as_secs_f64() as f64 * 1000.0;
    // let a = Duration::from_secs_f64(sum.as_secs_f64() as f64 / times.len() as f64);
    // println!("min: {:?} ms  max: {:?} ms avg: {:?} ms", min, max, a);

    Ok(())
}
