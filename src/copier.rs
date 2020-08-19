use std::io::{Read, Write, ErrorKind, BufWriter, BufReader};
use std::io;
use std::thread::spawn;
use anyhow::{anyhow, Context};
use std::any::Any;
use log::{debug, error, info, trace, warn, Record};
use std::sync::{Arc, Mutex};
use ssh2::File;

type Result<T> = anyhow::Result<T, anyhow::Error>;


pub fn copier(p_reader: &mut Arc<Mutex<BufReader<File>>>, p_writer: &mut Arc<Mutex<BufWriter<File>>>, buff_size: usize, buff_ring_size: usize) -> Result<usize> {
    let (_r_send, _w_recv) = crossbeam_channel::unbounded::<Option<(usize, Vec<u8>)>>();
    let (_w_send, _r_recv) = crossbeam_channel::unbounded::<Option<Vec<u8>>>();

    let mut t_reader = p_reader.clone();
    let mut t_writer = p_writer.clone();

    // prime the circle of buffers
    for _ in 0..buff_ring_size {
        _w_send.send(Some(vec![0u8; buff_size])).context("send in priming of copier routine failed")?;
    }

    let r_send = _r_send.clone();
    let w_recv = _w_recv.clone();

    let w_send = _w_send.clone();
    let r_recv = _r_recv.clone();


    let l_reader = spawn(move || -> usize {
        let mut written = 0;
        let mut reader = t_reader.lock().expect("cannot lock reader in reader thread");
        loop {
            match r_recv.recv().expect("recv of reader thread failed") {
                Some(mut buf) => {
                    let len = match reader.read(&mut buf) {
                        Ok(len) => len,
                        Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                        Err(e) => {
                            error!("error in reading thread leading to panice");
                            panic!("error in reader 1");
                        }
                    };
                    written += len;
                    if len == 0 {
                        r_send.send(None).expect("sending none to writer thread");
                        return written;
                    } else {
                        r_send.send(Some((len, buf))).expect("send of reader thread failed");
                    }
                }
                None => return written,
            }
        }
    });

    let l_writer = spawn(move || {
        let mut writer = t_writer.lock().expect("cannot locker writer in writer thread");
        loop {
            match w_recv.recv().expect("recv in writer thread failed") {
                Some((len, buf)) => {
                    if len == 0 {
                        return ();
                    } else {
                        writer.write_all(&buf[..len]).expect("writer in writer thread failed");
                        w_send.send(Some(buf)).expect("send in send thread failed");
                    }
                }
                None => return (),
            }
        }
    });

    match (l_reader.join(), l_writer.join()) {
        (Err(er), Err(ew)) => Err(anyhow!("error during transfer on read and writer thread: r: {:?} w: {:?}",&er, &ew))?,
        (Err(er), Ok(_)) => Err(anyhow!("error during transfer on read thread: {:?}",&er))?,
        (Ok(bytes_read), Err(ew)) => Err(anyhow!("error during transfer on writer thread after reading: {} bytes: error: {:?}",bytes_read, &ew))?,
        (Ok(written), Ok(_)) => Ok(written),
    }
}
