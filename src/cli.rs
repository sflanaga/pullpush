use anyhow::{anyhow, Context};
use structopt::StructOpt;
use url::Url;
use std::path::PathBuf;
use std::time::Duration;
use regex::Regex;

type Result<T> = std::result::Result<T, anyhow::Error>;

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

    #[structopt(long, parse(try_from_str = to_duration))]
    /// timeout in seconds
    pub timeout: Duration,

    #[structopt(long, parse(try_from_str = to_duration))]
    /// max age to consider for transfer
    pub max_age: Duration,

    #[structopt(long, parse(try_from_str = to_duration))]
    /// minimum age to consider for transfer
    pub min_age: Duration,

    #[structopt(long, parse(try_from_str = to_duration))]
    /// max age to keep in tracking file
    pub max_track_age: Duration,

    #[structopt(short="v", parse(from_occurrences))]
    /// log level
    pub verbosity: usize,

    #[structopt(short="v", default_value("65536"))]
    /// log level
    pub copy_buffer_size: usize,
}

fn to_duration(s: &str) -> Result<Duration> {
    let mut num = String::new();
    let mut sum_secs = 0u64;
    for c in s.chars() {
        if c >= '0' && c <='9' {
            num.push(c);
        } else {
            let s = num.parse::<u64>().with_context(|| format!("cannot parse number {} inside duration {}", &num, &s))?;
            num.clear();
            match c {
                's' => sum_secs += s,
                'm' => sum_secs += s*60,
                'h' => sum_secs += s * 3600,
                'd' => sum_secs += s*3600*24,
                'w' => sum_secs += s*3600*24*7,
                _ => Err(anyhow!("Cannot interpret {} as a time unit inside duration {}", c, &s))?,
            }
        }
    }
    if num.len() > 0 {
        sum_secs += num.parse::<u64>().with_context(|| format!("cannot parse number {} inside duration {}", &num, &s))?;
    }
    Ok(Duration::from_secs(sum_secs))
}