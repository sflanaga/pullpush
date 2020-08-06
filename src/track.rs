#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use std::path::{PathBuf, Path};
use std::collections::BTreeSet;
use ssh2::FileStat;
use anyhow::{Error, Context, anyhow};
use std::io::{BufWriter, BufRead, Write};
use std::fs::{File, remove_file};
use std::time::Instant;
use log::{info, debug, warn, error, trace};
use std::cmp::Ordering;
use log::Level::Trace;
use url::form_urlencoded::Target;

type Result<T> = anyhow::Result<T, anyhow::Error>;

#[derive(Debug, Eq, Clone)]
struct Track {
    src_path: PathBuf,
    lastmod: u64,
    size: u64,
}

impl PartialEq for Track {
    fn eq(&self, other: &Self) -> bool {
        self.src_path.cmp(&other.src_path) == Ordering::Equal
    }
}

impl PartialOrd for Track {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.src_path.cmp(&other.src_path))
    }
}

impl Ord for Track {
    fn cmp(&self, other: &Self) -> Ordering {
        self.src_path.cmp(&other.src_path)
    }
}


impl Track {
    pub fn from_str(s: &str) -> Result<Self> {
        let v = s.split('\0').collect::<Vec<_>>();
        if v.len() != 3 { Err(anyhow!("Missing fields in line \"{}\"", s))? }
        Ok(Track {
            src_path: PathBuf::from(v[0]),
            lastmod: v[1].parse().with_context(|| format!("last mod time number cannot be parsed in \"{}\"", s))?,
            size: v[2].parse().with_context(|| format!("file size number cannot be parsed in \"{}\"", s))?,
        })
    }
    pub fn from_sftp_entry(path: &PathBuf, filestat: &FileStat) -> Result<Self> {
        if filestat.mtime.is_none() || filestat.size.is_none() {
            Err(anyhow!("sftp file entry provided {:?} does not have size or date attached to it", (&path, &filestat)))?
        }

        Ok(Track {
            src_path: path.clone(),
            lastmod: filestat.mtime.unwrap(),
            size: filestat.size.unwrap(),
        })
    }
    pub fn write(&self, f: &mut dyn Write) {
        write!(f, "{}\0{}\0{}\n", self.src_path.display(), self.lastmod, self.size);
    }
}


pub struct Tracker {
    set: BTreeSet<Track>,
    file: PathBuf,
    wal: BufWriter<File>,
}

#[derive(Debug, Clone)]
pub enum TrackDelta {
    Equal,
    None,
    SizeChange,
    LastModChange,
}

impl Tracker {
    pub fn new(file: &PathBuf) -> Result<Self> {
        let mut set = BTreeSet::new();
        Tracker::entries_from(&file, &mut set)?;

        let mut filename = file.file_name().unwrap().to_owned();
        filename.push(".wal");
        let logpath = file.with_file_name(filename);

        // recover wal file
        if logpath.exists() {
            Tracker::entries_from(&logpath, &mut set)?;
            warn!("existing wal file: {}, read - so writing new tracker file to prevent further issues", &logpath.display());
            Tracker::write_entries(file, &set)?;
            remove_file(&logpath)?;
            info!("removed existing wal file");
        }
        let wal = BufWriter::new(std::fs::File::create(&logpath)
            .with_context(|| format!("Unable to create WAL log file\"{}\"", &logpath.display()))?);

        Ok(Tracker {
            file: file.clone(),
            wal,
            set,
        })
    }

    pub fn commit(&mut self) -> Result<()> {
        Tracker::write_entries(&self.file, &self.set)?;
        drop(&self.wal);

        let mut filename = self.file.file_name().unwrap().to_owned();
        filename.push(".wal");
        let logpath = self.file.with_file_name(filename);
        remove_file(&logpath)?;

        Ok(())
    }

    fn write_entries(path: &PathBuf, set: &BTreeSet<Track>) -> Result<()> {
        let mut tmppath = path.clone();
        let mut filename = String::from(".tmp_");
        filename.push_str(path.file_name().unwrap().to_str().unwrap());
        tmppath.pop();
        tmppath.push(filename);
        { // this scope forces drop of file for renaming
            let file = File::create(&tmppath)
                .with_context(|| format!("Unable to create tmpfile: \"{}\" to write tracking data too", &tmppath.display()))?;
            let mut buf = BufWriter::new(&file);
            for e in set {
                e.write(&mut buf);
            }
        }
        std::fs::rename(&tmppath, &path)
            .with_context(|| format!("Unable to post rename tmp file after writing tracking information: rename \"{}\" to \"{}\"", &tmppath.display(), &path.display()))?;
        Ok(())
    }

    fn entries_from(path: &PathBuf, set: &mut BTreeSet<Track>) -> Result<()> {
        let f_h = match File::open(&path) {
            Err(e) =>{
                warn!("There is no initial tracking file at \"{}\", so going with an initial empty one. {}", path.display(),e);
                return Ok(())
            },
            Ok(f) => f,
        };
        let lines = std::io::BufReader::new(f_h).lines();
        let mut count = 0;
        for l in lines {
            count += 1;
            let l = l.with_context(|| format!("unable parse data file:{}:{}", &path.display(), count))?;
            match Track::from_str(&l) {
                Err(e) => error!("skipping a line due to {}", e),
                Ok(t) => {
                    set.insert(t);
                    ()
                }
            }
        }
        info!("read {} entries form \"{}\"", count, &path.display());
        Ok(())
    }

    pub fn check(&mut self, path: &PathBuf, filestat: &FileStat) -> Result<TrackDelta> {
        let track = Track::from_sftp_entry(&path, &filestat)?;
        match self.set.get(&track) {
            None => Ok(TrackDelta::None),
            Some(e) => {
                if e.size != track.size {
                    Ok(TrackDelta::SizeChange)
                } else if e.lastmod != track.lastmod {
                    Ok(TrackDelta::LastModChange)
                } else {
                    Ok(TrackDelta::Equal)
                }
            }
        }
    }

    pub fn xferred(&mut self, path: &PathBuf, filestat: &FileStat) -> Result<()> {
        let track = Track::from_sftp_entry(&path, &filestat)
            .with_context(|| anyhow!("Not able to add entry to tracker: {:?}", (&path, &filestat)))?;
        track.write(&mut self.wal);
        self.wal.flush();
        self.set.insert(track);
        Ok(())
    }
}

fn read_lines(filename: &Path) -> Result<std::io::Lines<std::io::BufReader<File>>> {
    let file = File::open(filename)
        .with_context(|| format!("Cannot open file {}", &filename.to_str().unwrap()))?;
    Ok(std::io::BufReader::new(file).lines())
}

