#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unreachable_code)]

use std::io::{Read, Write};
use std::thread::{spawn, JoinHandle};
use anyhow::{anyhow, Context};
#[allow(unused_imports)]
use log::{debug, error, info, trace, warn, Record};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;
use crossbeam_channel::{Receiver, Sender};

type Result<T> = anyhow::Result<T, anyhow::Error>;

struct Copier {
    read_h_s: Sender<Arc<Mutex<Box<dyn Read + Send>>>>,
    write_h_s: Sender<Arc<Mutex<Box<dyn Write + Send>>>>,
    read_h_r: Receiver<Arc<Mutex<Box<dyn Read + Send>>>>,
    write_h_r: Receiver<Arc<Mutex<Box<dyn Write + Send>>>>,

    buff_size: usize,
    buff_ring_size: usize,
    read_s: Sender<Option<(usize, Vec<u8>)>>,
    write_r: Receiver<Option<(usize, Vec<u8>)>>,
    read_r: Receiver<Option<Vec<u8>>>,
    write_s: Sender<Option<Vec<u8>>>,

    res_s: Sender<usize>,
    res_w: Receiver<usize>,

    read_t: JoinHandle<()>,
    write_t: JoinHandle<()>,
}

impl Copier {
    pub fn new(buff_size: usize, buff_ring_size: usize) -> Self {
        //hmmmm... is this worth is?
        let (read_h_s, read_h_r) = crossbeam_channel::unbounded();
        let (write_h_s, write_h_r) = crossbeam_channel::unbounded();
        let (read_s, write_r) = crossbeam_channel::unbounded::<Option<(usize, Vec<u8>)>>();
        let (write_s, read_r) = crossbeam_channel::unbounded::<Option<Vec<u8>>>();
        let (res_s, res_w) = crossbeam_channel::unbounded();

        let read_t = {
            let file_recv = read_h_r.clone();
            let buff_send = read_s.clone();
            let buff_recycle = read_r.clone();
            spawn(move || reader_thread(file_recv, buff_send, buff_recycle))
        };

        let write_t = {
            let file_recv = write_h_r.clone();
            let buff_send = write_r.clone();
            let buff_recycle = write_s.clone();
            spawn(move || writer_thread(file_recv, buff_send, buff_recycle))
        };

        for _ in 0..buff_ring_size {
            write_s.send(Some(vec![0u8; buff_size])).context("send in priming of copier routine failed").unwrap();
        }

        Copier{
            read_h_s,
            write_h_s,
            read_h_r,
            write_h_r,
            buff_size,
            buff_ring_size,
            read_s,
            write_r,
            read_r,
            write_s,
            res_s,
            res_w,
            read_t,
            write_t
        }
    }
}

fn reader_thread(file_recv: Receiver<Arc<Mutex<Box<dyn Read + Send>>>>,
                 buff_send:  Sender<Option<(usize, Vec<u8>)>>,
                 buff_recycle: Receiver<Option<Vec<u8>>>) -> () {


    ()
}

fn writer_thread(file_recv: Receiver<Arc<Mutex<Box<dyn Write + Send>>>>,
                 buff_recv:  Receiver<Option<(usize, Vec<u8>)>>,
                 buff_recycle: Sender<Option<Vec<u8>>>) -> () {

}

fn fill_buff(handle: &mut MutexGuard<dyn Read>, buff: &mut [u8]) -> Result<usize> {
    // eprintln!("call fill");
    let mut sz = handle.read(&mut buff[..])?;
    // eprintln!("mid read: {}", sz);
    loop {
        if sz == 0 {
            return Ok(sz);
        } else if sz == buff.len() {
            return Ok(sz);
        }

        let sz2 = handle.read(&mut buff[sz..])?;
        // eprintln!("mid2 read: {}", sz2);

        if sz2 == 0 {
            return Ok(sz);
        } else {
            sz += sz2;
        }
    }
}

pub fn copier(p_reader: &mut Arc<Mutex<Box<dyn Read + Send>>>, p_writer: &mut Arc<Mutex<Box<dyn Write + Send>>>, buff_size: usize, buff_ring_size: usize) -> Result<usize> {
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
            let now = Instant::now();
            match r_recv.recv().expect("recv of reader thread failed") {
                Some(mut buf) => {
                    let afterrecv = now.elapsed().as_micros();
                    let len = reader.read(&mut buf[..]).context("fail on regular read").expect("failure in the middle of read in copier thread");
                    let afterread = now.elapsed().as_micros();
                    written += len;
                    if len == 0 {
                        r_send.send(None).expect("sending none to writer thread");
                        return written;
                    } else {
                        r_send.send(Some((len, buf))).expect("send of reader thread failed");
                    }
                    let aftersend = now.elapsed().as_micros();
                    trace!("read: {}  waittime: {}  readtime: {}  sendtime: {}", len, afterrecv, (afterread-afterrecv), (aftersend-afterread));
                }
                None => return written,
            }
        }
    });

    let l_writer = spawn(move || {
        let mut writer = t_writer.lock().expect("cannot locker writer in writer thread");
        loop {
            let now = Instant::now();
            match w_recv.recv().expect("recv in writer thread failed") {
                Some((len, buf)) => {
                    if len == 0 {
                        return ();
                    } else {
                        let waittime = now.elapsed().as_micros();
                        writer.write_all(&buf[..len]).expect("writer in writer thread failed");
                        let afterwrite = now.elapsed().as_micros();
                        w_send.send(Some(buf)).expect("send in send thread failed");
                        let aftersend = now.elapsed().as_micros();
                        trace!("wrote: {}  waittime: {}  writetime: {}  sendtime: {}", len, waittime, (afterwrite-waittime), (aftersend-afterwrite));
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
