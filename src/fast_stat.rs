use anyhow::{anyhow, Context};
use log::{debug, error, info, trace, warn, Record};
use crossbeam_channel::{Receiver, Sender};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use crate::vfs::FileStatus;
use std::thread::spawn;
use std::convert::TryFrom;
use std::sync::atomic::Ordering;

type Result<T> = anyhow::Result<T, anyhow::Error>;

fn get_stats(recv: &Receiver<Option<PathBuf>>, list: &mut Arc<Mutex<Option<Vec<(PathBuf, FileStatus)>>>>) -> () {
    match __get_stats(recv, list) {
        Err(e) => {
            error!("error in fast stats thread: {}", e);
            ()
        },
        Ok(())=>()
    }
}

fn __get_stats(recv: &Receiver<Option<PathBuf>>, list: &mut Arc<Mutex<Option<Vec<(PathBuf, FileStatus)>>>>) -> Result<()> {
    loop {
        match recv.recv() {
            Err(e) => return Err(anyhow!("cannot recv in stats thread {}", e)),
            Ok(None) => break,
            Ok(Some(path)) => {
                crate::STATS.stat_check.fetch_add(1, Ordering::Relaxed);
                let md = std::fs::metadata(&path)?;
                let fs = FileStatus::try_from(&md)?;
                match list.lock() {
                    Err(e) => return Err(anyhow!("cannot lock list in file stats thread {}", e)),
                    Ok(mut l) => l.as_mut().unwrap().push((path, fs)),
                }
            }
        }
    }
    Ok(())

}

pub fn get_stats_fast(no_threads: usize, list: &mut Vec<PathBuf>) -> Result<Vec<(PathBuf,FileStatus)>> {

    let mut results = Arc::new(Mutex::new(Some(vec![])));
    {
        let (s, r) = crossbeam_channel::unbounded();
        let mut vec_h = vec![];
        for t in 0..no_threads {
            let r_c = r.clone();
            let mut res_c = results.clone();
            let h = spawn(move || get_stats(&r_c, &mut res_c));
            vec_h.push(h);
        }

        loop {
            match list.pop() {
                None => break,
                Some(p) => {
                    trace!("sending file off to get stat {}", p.display());
                    s.send(Some(p))?
                },
            }
        }

        for t in 0..no_threads {
            s.send(None)?;
        }

        for h in vec_h {
            h.join().unwrap();
        }
    }
    let results = results.lock().unwrap().take().unwrap();
    Ok(results)
}