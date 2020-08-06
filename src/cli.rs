use anyhow::{anyhow, Context};
use structopt::StructOpt;
use url::Url;
use std::path::PathBuf;
use std::time::Duration;
use regex::Regex;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(StructOpt, Debug, Clone)]
#[structopt(
rename_all = "kebab-case",
global_settings(& [
structopt::clap::AppSettings::ColoredHelp,
structopt::clap::AppSettings::UnifiedHelpMessage
]),
)]
pub struct Cli {
    #[structopt(long)]
    /// source url
    pub src_url: Url,

    #[structopt(long)]
    /// destiation url
    pub dst_url: Url,

    #[structopt(long)]
    /// source private key files
    pub src_pk: PathBuf,

    #[structopt(long)]
    /// destination private key files
    pub dst_pk: PathBuf,

    #[structopt(long)]
    /// regular expression on filename along to filter with
    pub re: Regex,

    #[structopt(long)]
    /// tracking list name
    pub track: PathBuf,

    #[structopt(long, parse(try_from_str = secs_to_dur))]
    /// timeout in seconds
    pub timeout: Duration,
}

fn secs_to_dur(s: &str) -> Result<Duration> {
    Ok(Duration::from_secs(s.parse::<u64>()?))
}