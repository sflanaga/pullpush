#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use anyhow::{anyhow as ERR, Context};
use log::{debug, error, info, trace, warn, Record};
use std::path::{PathBuf, Path};
use ssh2::{Sftp, Session, FileStat};
use libssh2_sys::LIBSSH2_ERROR_FILE;
use std::fs::{ReadDir, Metadata};
use std::io::{Write, Read};
use url::Url;
use std::time::{Duration, SystemTime};
use std::net::TcpStream;
use crate::vfs::FileType::{Regular, Directory};
use std::convert::TryFrom;
use std::ops::Add;
use std::fmt::Display;
use std::sync::atomic::Ordering;

type Result<T> = anyhow::Result<T, anyhow::Error>;

struct SftpFile {
    path: PathBuf,
    file: ssh2::File,
}

struct LocalFile {
    path: PathBuf,
    itr: ReadDir,
}


#[derive(Clone,Copy,Debug,Eq,PartialEq)]
pub enum FileType {
    Regular,
    Directory,
    Unknown,
}

#[derive(Clone,Copy,Debug)]
pub struct FileStatus {
    pub file_type: FileType,
    pub size: u64,
    pub mtime: SystemTime,
}

struct SftpVfs {
    write_perm: Option<u32>,
    base_dir: PathBuf,
    sftp: Sftp,
}

struct LocalVfs {
    base_dir: PathBuf,
}

pub enum Vfs {
    Sftp(SftpVfs),
    Local(LocalVfs)
}

pub enum ReadDirHandle {
    Local(LocalFile),
    Sftp(SftpFile),
}

impl ReadDirHandle {
    pub fn read_all_dir_entry(&mut self) -> Result<Vec<(PathBuf, Option<FileStatus>)>> {
        match self {
            ReadDirHandle::Sftp(h) => {
                let mut list = vec![];
                let (this_dir, par_dir) = (Path::new("."), Path::new(".."));
                loop {
                    match h.file.readdir() {
                        Ok((filename, stat)) => {
                            if &*filename == this_dir || &*filename == par_dir {
                                continue;
                            }

                            let status = FileStatus::try_from(&stat).context("next_dir_entry of SftpFile canon")?;
                            trace!("next_dir_entry sftp return: {}", filename.display());
                            crate::STATS.dirs_check.fetch_add(1, Ordering::Relaxed);
                            list.push( (filename, Some(status)) );
                        }
                        Err(ref e) if e.code() == LIBSSH2_ERROR_FILE => return Ok(list),
                        Err(e) => return Err(ERR!("error on next readdir: {}", e)),
                    };
                }

            },
            ReadDirHandle::Local(h) => {
                let mut list = vec![];
                loop {
                    match h.itr.next() {
                        None => return Ok(list),
                        Some(r) => match r {
                            Err(e) => return Err(ERR!("error on reading next entry in ReadDir: {}", e)),
                            Ok(de) => {
                                crate::STATS.dirs_check.fetch_add(1, Ordering::Relaxed);
                                list.push((de.path(), None));
                            },
                        }
                    }
                }
            },
        }
    }

}

impl Vfs {
    pub fn new(url: &Url, perm: Option<u32>, pk: &Option<PathBuf>, timeout: Option<Duration>) -> Result<Vfs> {
        match url.scheme() {
            "sftp" => {
                match (pk, timeout) {
                    (Some(pk), Some(timeout)) => {
                        let soc = url.socket_addrs(|| Some(22))?[0];
                        let tcp = TcpStream::connect_timeout(&soc, timeout).with_context(|| format!("Tcp connection to url: {} failed", &url))?;


                        let mut sess = Session::new().unwrap();
                        sess.set_tcp_stream(tcp);
                        sess.handshake()?;
                        sess.userauth_pubkey_file(&url.username(), None,
                                                  &pk, None).with_context(|| format!("Unable to setup user with private key: {} for url {}", pk.display(), &url))?;

                        let sftp = sess.sftp().with_context(|| format!("Unable to create sftp session with private key: {} for url {}", pk.display(), &url))?;
                        sftp.lstat(&*PathBuf::from(&url.path().to_string())).with_context(|| format!("Cannot stat check remote path of \"{}\"", url))?;
                        info!("creating sftp vfs for {}", &url);
                        return Ok(Vfs::Sftp(SftpVfs {
                            base_dir: PathBuf::from(url.path()),
                            sftp: sftp,
                            write_perm: perm,
                        }));
                    }
                    _ => return Err(ERR!("sftp URL requires timeout and private key settings for {}", url)),
                }
            }
            "file" => {
                info!("creating file vfs for {}", url);
                return Ok(Vfs::Local(LocalVfs {
                    base_dir: PathBuf::from(url.path())
                }));
            }
            _ => return Err(ERR!("Cannot create an file or sftp based VFS from url: {}", &url)),
        }
    }

    pub fn base_dir(&self) -> &PathBuf {
        match self {
            Vfs::Sftp(f) => &f.base_dir,
            Vfs::Local(f) => &f.base_dir,
        }
    }


    pub fn open_dir(&mut self, path: &Path) -> Result<ReadDirHandle> {
        match self {
            Vfs::Sftp(f) => {
                let file = ReadDirHandle::Sftp(SftpFile { path: path.to_path_buf(), file: f.sftp.opendir(path.as_ref())? });
                Ok(file)
            },
            Vfs::Local(f) => {
                let r = std::fs::read_dir(&path).context(line!())?;
                Ok(ReadDirHandle::Local(LocalFile { path: path.to_path_buf(), itr: r }))
            },
        }

    }
    pub fn open(&self, filename: &Path) -> Result<Box<dyn Read + Send>> {
        match self {
            Vfs::Sftp(f) => Ok(Box::new(f.sftp.open(filename)?)),
            Vfs::Local(f) => Ok(Box::new(std::fs::File::open(&filename)?)),
        }
    }
    pub fn create(&self, filename: &Path) -> Result<Box<dyn Write + Send>> {
        match self {
            Vfs::Sftp(f) => Ok(Box::new(f.sftp.create(filename)?)),
            Vfs::Local(f) => Ok(Box::new(std::fs::File::create(&filename)?)),
        }
    }
    pub fn set_perm(&self, path: &Path) -> Result<()> {
        match self {
            Vfs::Sftp(f) => Ok(f.sftp.setstat(&path, FileStat { perm: f.write_perm, mtime: None, size: None, atime: None, gid: None, uid: None })?),
            Vfs::Local(f) => Ok(()),
        }
    }
    pub fn rename(&self, src: &Path, dst: &Path) -> Result<()> {
        match self {
            Vfs::Sftp(f) => Ok(f.sftp.rename(src, dst, None)?),
            Vfs::Local(f) => Ok(std::fs::rename(src, dst)?),
        }
    }
    pub fn stat(&self, path: &Path) -> Result<FileStatus> {
        match self {
            Vfs::Sftp(f) => Ok(FileStatus::try_from(&f.sftp.lstat(path)?)?),
            Vfs::Local(f) => Ok(FileStatus::try_from(&std::fs::metadata(&path)?)?),
        }
    }

}



impl TryFrom<&std::fs::Metadata> for FileStatus {
    type Error = std::io::Error;
    fn try_from(value: &Metadata) -> std::result::Result<Self, Self::Error> {
        let ft = value.modified()?;
        Ok(FileStatus {
            file_type: if value.is_file() {
                Regular
            } else {
                Directory
            },
            size: value.len(),
            mtime: ft,
        })
    }
}

impl TryFrom<&ssh2::FileStat> for FileStatus {
    type Error = ssh2::Error;

    fn try_from(value: &FileStat) -> std::result::Result<Self, Self::Error> {
        Ok(FileStatus {
            file_type: if value.is_dir() {
                Directory
            } else {
                Regular
            },
            mtime: SystemTime::UNIX_EPOCH.add(Duration::from_secs(value.mtime.unwrap())),
            size: value.size.unwrap_or(0),
        })
    }
}

