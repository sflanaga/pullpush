#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unreachable_code)]


use std::path::PathBuf;
use structopt::StructOpt;
use std::time::Instant;
use anyhow::{anyhow, Context};
use log::{debug, error, info, trace, warn};
use log::LevelFilter;
use lazy_static::lazy_static;

mod util;

use crate::util::{to_log_level, init_log};

type Result<T> = anyhow::Result<T, anyhow::Error>;

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
pub struct Cli {
    #[structopt(long, default_value("100000"))]
    /// how much to pre-allocated in time tracking vector
    pub vec_pre_alloc_size: usize,

    #[structopt()]
    /// path to the directory to read
    pub path: PathBuf,

    #[structopt(short, default_value("10"))]
    /// limit the detailed output
    pub limit_detailed_output: usize,

    #[structopt(short, long)]
    /// get metadata with each dir entry
    pub turn_on_meta_data: bool,

    #[structopt(short, long)]
    /// join and normalize the path for each dir entry
    pub canonicalize: bool,

    #[structopt(short = "D", long, parse(try_from_str = to_log_level), default_value("info"))]
    /// log level
    pub log_level: LevelFilter,
}

fn main() {
    match run() {
        Err(e) => error!("got high error: {:?}", e),
        Ok(()) => (),
    }
}

fn run() -> Result<()> {
    let cli:Cli = Cli::from_args();
    util::init_log(cli.log_level);

    let mut data = Vec::with_capacity(cli.vec_pre_alloc_size);

    let start_f = Instant::now();

    let mut r = std::fs::read_dir(&cli.path).context(line!())?;
    loop {
        match r.next() {
            None => break,
            Some(r) => match r {
                Err(e) => return Err(anyhow!("error on reading next entry in ReadDir: {}", e)),
                Ok(de) => {
                    //let full = cli.path.join(de.path());
                    let stat = std::fs::metadata(de.path())?;
                    data.push( (de.path().clone(), stat) );
                },
            }
        }
    }
    for e in data.iter().enumerate() {
        if e.0 > cli.limit_detailed_output {
            println!("...");
            break;
        }
        println!("{}  {:?}", (e.1).0.display(), (e.1).1);
    }

    info!("read {} files and status in {:?}", data.len(), start_f.elapsed());

    Ok(())
}


