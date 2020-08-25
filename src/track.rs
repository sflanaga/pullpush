// #![allow(dead_code)]
// #![allow(unused_imports)]
// #![allow(unused_variables)]
//
use std::path::{PathBuf};
use std::collections::BTreeSet;
use ssh2::FileStat;
use anyhow::{Context, anyhow};
use std::io::{BufWriter, BufRead, Write};
use std::fs::{File, remove_file};
use std::time::{SystemTime, Duration};
use log::{info, debug, warn, error, trace};
use std::cmp::Ordering;
use std::ops::Add;

use crate::vfs::{FileStatus};


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

fn u64_to_SystemTime(mtime: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH.add(Duration::from_secs(mtime))
}

fn SystemTime_to_u64(mtime: SystemTime) -> u64 {
    let dur = mtime.duration_since(SystemTime::UNIX_EPOCH).unwrap_or(Duration::from_secs(0));
    dur.as_secs()
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
    pub fn from_sftp_entry(path: &PathBuf, filestat: FileStatus) -> Result<Self> {
        Ok(Track {
            src_path: path.clone(),
            lastmod: SystemTime_to_u64(filestat.mtime),
            size: filestat.size,
        })
    }
    fn from_just_path(path: &PathBuf) -> Self {
        Track {
            src_path: path.clone(),
            lastmod: 0,
            size: 0,
        }
    }


    pub fn write(&self, f: &mut dyn Write) -> Result<()> {
        write!(f, "{}\0{}\0{}\n", self.src_path.display(), self.lastmod, self.size)?;
        Ok(())
    }
}


pub struct Tracker {
    set: BTreeSet<Track>,
    file: PathBuf,
    wal: Option<BufWriter<File>>,
}

#[derive(Debug, Clone)]
pub enum TrackDelta {
    Equal,
    None,
    SizeChange,
    LastModChange,
}

impl Tracker {
    pub fn new(file: &PathBuf, max_track_age: Duration) -> Result<Self> {
        let mut set = BTreeSet::new();
        Tracker::entries_from(&file, &mut set, max_track_age)?;

        let mut filename = file.file_name().unwrap().to_owned();
        filename.push(".wal");
        let logpath = file.with_file_name(filename);

        // recover wal file
        if logpath.exists() {
            Tracker::entries_from(&logpath, &mut set, max_track_age)?;
            warn!("existing wal file: {}, read - so writing new tracker file to prevent further issues", &logpath.display());
            Tracker::write_entries(file, &set)?;
            remove_file(&logpath)?;
            info!("removed existing wal file");
        }
        let wal = BufWriter::new(std::fs::File::create(&logpath)
            .with_context(|| format!("Unable to create WAL log file\"{}\"", &logpath.display()))?);

        Ok(Tracker {
            file: file.clone(),
            wal: Some(wal),
            set,
        })
    }

    pub fn commit(&mut self) -> Result<()> {
        Tracker::write_entries(&self.file, &self.set)?;

        let mut filename = self.file.file_name().unwrap().to_owned();
        filename.push(".wal");
        let logpath = self.file.with_file_name(filename);
        self.wal = None; // should close the file....
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
                e.write(&mut buf)?;
            }
        }
        std::fs::rename(&tmppath, &path)
            .with_context(|| format!("Unable to post rename tmp file after writing tracking information: rename \"{}\" to \"{}\"", &tmppath.display(), &path.display()))?;
        Ok(())
    }

    fn entries_from(path: &PathBuf, set: &mut BTreeSet<Track>, max_track_age: Duration) -> Result<()> {
        let now = SystemTime::now();

        let f_h = match File::open(&path) {
            Err(e) => {
                warn!("There is no initial tracking file at \"{}\", so going with an initial empty one. {}", path.display(), e);
                return Ok(());
            }
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
                    let ft = SystemTime::UNIX_EPOCH.add(Duration::from_secs(t.lastmod));
                    let age = now.duration_since(ft).with_context(|| format!("mtime calc: mtime: {}, now: {:?} ft: {:?}", t.lastmod, now, ft))?;
                    if age < max_track_age {
                        trace!("file \"{}\" tracking age: {:?}", t.src_path.display(), age);
                        set.insert(t);
                    } else {
                        trace!("file \"{}\" too old at {:?}", t.src_path.display(), age);
                    }
                    ()
                }
            }
        }

        info!("read {} entries from \"{}\" in {:?}", count, &path.display(), now.elapsed().unwrap_or(Duration::from_secs(0)));
        Ok(())
    }

    pub fn path_exists_in_tracker(&self, path: &PathBuf) -> bool {
        let track = Track::from_just_path(&path);
        return self.set.contains(&track)
    }

    pub fn check(&mut self, path: &PathBuf, filestat: FileStatus) -> Result<TrackDelta> {
        let track = Track::from_sftp_entry(&path, filestat)?;
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

    /// Here we just want to record the path in the tracker so
    /// we can filter it fast later.
    /// We do no flushing of buffers here om the WAL
    pub fn insert_path(&mut self, path: &PathBuf) -> Result<()> {
        let fs = FileStatus {
            mtime: SystemTime::UNIX_EPOCH,
            size: 0,
            file_type: crate::vfs::FileType::Unknown,
        };
        let track = Track::from_sftp_entry(&path, fs)
            .with_context(|| anyhow!("Not able to record just path to tracker: {:?}", (&path, fs)))?;
        self.set.insert(track);
        Ok(())
    }

    pub fn insert_path_and_status(&mut self, path: &PathBuf, filestat: FileStatus) -> Result<()> {
        let track = Track::from_sftp_entry(&path, filestat)
            .with_context(|| anyhow!("Not able to record just path to tracker: {:?}", (&path, filestat)))?;
        self.set.insert(track);
        Ok(())
    }

    pub fn xferred(&mut self, path: &PathBuf, filestat: FileStatus) -> Result<()> {
        let track = Track::from_sftp_entry(&path, filestat)
            .with_context(|| anyhow!("Not able to add entry to tracker: {:?}", (&path, filestat)))?;
        track.write(self.wal.as_mut().unwrap())?;
        self.set.insert(track);
        self.wal.as_mut().unwrap().flush()?;
        Ok(())
    }
}

