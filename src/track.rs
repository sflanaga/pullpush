// #![allow(dead_code)]
// #![allow(unused_imports)]
// #![allow(unused_variables)]
//
use std::path::{PathBuf};
use anyhow::{Context, anyhow};
use std::io::{BufWriter, BufRead, Write};
use std::fs::{File, remove_file};
use std::time::{SystemTime, Duration, Instant};
#[allow(unused_imports)]
use log::{info, debug, warn, error, trace};
use std::cmp::Ordering;
use std::ops::{Add, Sub};
use crate::vfs::{FileStatus};
use std::hash::Hasher;

//use hashbrown::HashSet - only add 5% so not using it
use std::collections::HashSet;

type Result<T> = anyhow::Result<T, anyhow::Error>;

#[derive(Debug, Eq, Clone)]
struct Track {
    src_path: PathBuf,
    lastmod: u64,
    size: u64,
}


impl std::hash::Hash for Track {
    fn hash<H: Hasher>(&self, state: &mut H) {
        //self.src_path.hash(state) - optimization but relies on path string being normalized
        // 22% increase in read tracker perf
        self.src_path.as_os_str().to_string_lossy().hash(state)
    }
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

fn u64_to_system_time(mtime: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH.add(Duration::from_secs(mtime))
}

fn system_time_to_u64(mtime: SystemTime) -> u64 {
    let dur = mtime.duration_since(SystemTime::UNIX_EPOCH).unwrap_or(Duration::from_secs(0));
    dur.as_secs()
}

fn to_err<T>(opt: Option<T>, msg: &'static str) -> Result<T> {
    match opt {
        None => Err(anyhow!(msg)),
        Some(t) => Ok(t)
    }
}

impl Track {
    pub fn from_str(s: &str) -> Result<Self> {
        let mut v = s.split('\0');
        Ok(Track {
            src_path: PathBuf::from(
                to_err(v.next(), "missing first field in track record")?
            ),
            lastmod: to_err(v.next(), "missing 2nd field in track record")?
                .parse()
                .with_context(|| format!("last mod time number cannot be parsed in \"{}\"", s))?,
            size: to_err(v.next(), "missing 3rd field in track record")?
                .parse()
                .with_context(|| format!("file size number cannot be parsed in \"{}\"", s))?,
        })
    }
    /*
    from_str.... opt notes:

    I tried smallvec, lexical::parse, csv parsing

    The main things that helped was using faster hasher, and not collecting the result of
    of the split above
    */

    pub fn from_sftp_entry(path: &PathBuf, filestat: FileStatus) -> Result<Self> {
        Ok(Track {
            src_path: path.clone(),
            lastmod: system_time_to_u64(filestat.mtime),
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
    set: HashSet<Track>,
    file: PathBuf,
    wal: Option<BufWriter<File>>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum TrackDelta {
    Equal,
    None,
    SizeChange,
    LastModChange,
}

impl Tracker {
    pub fn new(file: &PathBuf, max_track_age: Duration) -> Result<Self> {
        let mut set = HashSet::default();
        Tracker::entries_from(&file, &mut set, max_track_age)?;

        let mut wal_filename = file.file_name().unwrap().to_owned();
        wal_filename.push(".wal");
        let wal_path = file.with_file_name(wal_filename);

        // recover wal file
        if wal_path.exists() {
            if std::fs::metadata(&wal_path)?.len() == 0 {
                warn!("wal file there but empty, so ignoring \"{}\"", &wal_path.display());
            } else {
                // note the wal may overwrite track entries which happened above
                // the wal may have later updates.
                Tracker::entries_from(&wal_path, &mut set, max_track_age)?;
                warn!("existing wal file: {}, read - so writing new tracker file to prevent further issues", &wal_path.display());
                Tracker::write_entries(file, &set)?;
                remove_file(&wal_path)?;
                info!("removed existing wal file");
            }
        }

        let wal = BufWriter::new(std::fs::File::create(&wal_path)
            .with_context(|| format!("Unable to create WAL log file\"{}\"", &wal_path.display()))?);

        Ok(Tracker {
            file: file.clone(),
            wal: Some(wal),
            set,
        })
    }

    #[allow(unused)]
    pub fn num_entries(&self) -> usize {
        self.set.len()
    }

    pub fn commit(&mut self) -> Result<()> {
        let start_f = Instant::now();
        Tracker::write_entries(&self.file, &self.set)?;

        let mut filename = self.file.file_name().unwrap().to_owned();
        filename.push(".wal");
        let logpath = self.file.with_file_name(filename);
        self.wal = None; // should close the file....
        remove_file(&logpath)?;
        info!("commited {} entries to track file {} in {:?}", self.set.len(), self.file.display(), start_f.elapsed());
        Ok(())
    }

    fn write_entries(path: &PathBuf, set: &HashSet<Track>) -> Result<()> {
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

    fn entries_from(path: &PathBuf, set: &mut HashSet<Track>, max_track_age: Duration) -> Result<()> {
        trace!("reading state file: {}", path.display());
        let now = SystemTime::now();

        let f_h = match File::open(&path) {
            Err(e) => {
                warn!("There is no initial tracking file at \"{}\", so going with an initial empty one. {}", path.display(), e);
                return Ok(());
            }
            Ok(f) => f,
        };
        let fs = std::fs::metadata(path)?.len();

        let mtime_too_old = {
            // compute mtime cutoff point so we do not repeat that computation
            // in inner loop
            let now = SystemTime::now();
            let then = now.sub(max_track_age);
            system_time_to_u64(then)
        };

        let lines = std::io::BufReader::new(f_h).lines();
        let mut count = 0;
        for l in lines {
            let l = l.with_context(|| format!("unable parse data file:{}:{}", &path.display(), count))?;
            match Track::from_str(&l) {
                Err(e) => error!("skipping a line due to {}", e),
                Ok(t) => {
                    if t.lastmod > mtime_too_old {
                        let lastmod = t.lastmod;
                        if set.contains(&t) {
                            trace!("replacing entry file \"{}\" tracking age: {:?}", &t.src_path.display(), u64_to_system_time(lastmod));
                            set.replace(t);
                        } else {
                            trace!("entry file \"{}\" tracking age: {:?}", t.src_path.display(), u64_to_system_time(lastmod));
                            set.insert(t);
                        }
                        count += 1;
                    } else {
                        trace!("file \"{}\" too old at {:?}", t.src_path.display(), u64_to_system_time(t.lastmod));
                    }
                    ()
                }
            }
        }
        if fs > 0 && count == 0 {
            return Err(anyhow!("Fishy tracker file: {}. It has size but no records could be parsed from it.", path.display()));
        }
        info!("read {} entries from \"{}\" in {:?}", count, &path.display(), now.elapsed().unwrap_or(Duration::from_secs(0)));
        Ok(())
    }


    pub fn path_exists_in_tracker(&self, path: &PathBuf) -> bool {
        let track = Track::from_just_path(&path);
        return self.set.contains(&track);
    }

    #[allow(unused)]
    pub fn check(&self, path: &PathBuf, filestat: FileStatus) -> Result<TrackDelta> {
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
    #[allow(unused)]
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
        self.set.replace(track);
        self.wal.as_mut().unwrap().flush()?;
        Ok(())
    }
}

