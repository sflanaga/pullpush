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

    #[structopt(long, default_value("644"), parse(try_from_str = to_perm))]
    /// destination permissions in octal
    pub dst_perm: i32,

    #[structopt(long)]
    /// regular expression on filename of files to keep
    ///
    /// ".*" means all filenames will pass
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

    #[structopt(short="v", parse(from_occurrences), conflicts_with("quiet"))]
    /// log level - e.g. -vvv is the same as debug while -vv is info level
    ///
    /// To true debug your settings you might try trace level or -vvvv
    pub verbosity: usize,

    #[structopt(long, default_value("1048576"), parse(try_from_str = to_size_usize))]
    /// Size of the buffer between pull and push connections e.g. 1M or 256k
    ///
    /// Performance vary widely based on this number.  The
    /// default is nice mid-way, but 64M might help.
    pub copy_buffer_size: usize,

    #[structopt(long)]
    /// Runs without actual xfer, read long help for more
    ///
    /// This is useful to do initial tests without waiting for transfer
    /// but also can be used to pre-populate the tracker with history
    /// so that you can start transferring files ONLY after you this
    /// dry_run and the run it normally.
    pub dry_run: bool,

    #[structopt(long, default_value="4")]
    /// Number of transfer threads and also connections used + 1 to source
    pub threads: usize,

    #[structopt(long)]
    /// Create sftp channels from a single session
    ///
    /// By default each transfer thread creates it's down session for performance.
    /// This changes things so that each thread re-uses a single session, but creates
    /// a seperate channel for each thread instaed.  This can slow down transfers,
    /// but minimizes the number of sessions to remote dest.
    /// While the channels do allow multiple exchanges in flight, they do seem
    /// to limit performance.
    pub reuse_sessions: bool,

    #[structopt(long)]
    /// Include hidden files or files starting with '.'
    ///
    /// By default hidden files are excluded
    pub include_dot_files: bool,

    #[structopt(long, conflicts_with("verbosity"))]
    /// Turn off any logging at all
    ///
    /// Even with things quiet there is still the tracker
    /// if you must find out what has been transferredA
    pub quiet: bool,
}

fn to_perm(s: &str) -> Result<i32> {
    Ok(i32::from_str_radix(&s, 8)?)
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

fn to_size_u64(s: &str) -> Result<u64> {
    let mut num = String::new();
    let mut bytes = 0u64;
    for c in s.chars() {
        if c >= '0' && c <='9' {
            num.push(c);
        } else {
            let s = num.parse::<u64>().with_context(|| format!("cannot parse number {} inside duration {}", &num, &s))?;
            num.clear();
            match c {
                'k' | 'K'  => bytes += s * 1024,
                'm' | 'M'  => bytes += s * (1024*1024),
                'g' | 'G' => bytes += s * (1024*1024*1024),
                't' | 'T' => bytes += s * (1024*1024*1024*1024),
                'p' | 'P' => bytes += s * (1024*1024*1024*1024*1024),
                _ => Err(anyhow!("Cannot interpret {} as a bytes unit inside size {}", c, &s))?,
            }
        }
    }
    if num.len() > 0 {
        bytes += num.parse::<u64>().with_context(|| format!("cannot parse number {} inside size {}", &num, &s))?;
    }
    Ok(bytes)
}

fn to_size_usize(s: &str) -> Result<usize> {
    let sz = to_size_u64(s)?;
    return Ok(sz as usize);
}
