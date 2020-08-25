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

type Result<T> = anyhow::Result<T, anyhow::Error>;


pub trait Vfs {
    fn base_dir(&self) -> &PathBuf;
    fn open_dir(&mut self, path: &Path) -> Result<Box<dyn VfsFile>>;
    fn open(&self, filename: &Path) -> Result<Box<dyn Read>>;
    fn create(&self, filename: &Path) -> Result<Box<dyn Write>>;
    fn set_perm(&self, path: &Path) -> Result<()>;
    fn rename(&self, src: &Path, dst: &Path) -> Result<()>;
    fn stat(&self, path: &Path) -> Result<FileStatus>;
}


pub fn create_vfs(url: &Url, perm: Option<u32>, pk: &Option<PathBuf>, timeout: Option<Duration>) -> Result<Box<dyn Vfs + Send>> {
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
                    return Ok(Box::new(SftpVfs {
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
            return Ok(Box::new(LocalVfs {
                base_dir: PathBuf::from(url.path())
            }));
        }
        _ => return Err(ERR!("Cannot create an file or sftp based VFS from url: {}", &url)),
    }
}
struct SftpVfs {
    write_perm: Option<u32>,
    base_dir: PathBuf,
    sftp: Sftp,
}

struct LocalVfs {
    base_dir: PathBuf,
}


impl Vfs for SftpVfs {
    fn open_dir(&mut self, path: &Path) -> Result<Box<dyn VfsFile>> {
        let file = Box::new(SftpFile { path: path.to_path_buf(), file: self.sftp.opendir(path.as_ref())? });

        Ok(file)
    }

    fn base_dir(&self) -> &PathBuf {
        &self.base_dir
    }

    fn open(&self, filename: &Path) -> Result<Box<dyn Read>> {
        Ok(Box::new(self.sftp.open(filename)?))
    }
    fn create(&self, filename: &Path) -> Result<Box<dyn Write>> {
        Ok(Box::new(self.sftp.create(filename)?))
    }

    fn set_perm(&self, path: &Path) -> Result<()> {
        if let Some(perm) = self.write_perm {
            let mut newstats = FileStat { perm: Some(perm), mtime: None, size: None, atime: None, gid: None, uid: None };
            self.sftp.setstat(&path, newstats)?;
        }
        Ok(())
    }
    fn rename(&self, src: &Path, dst: &Path) -> Result<()> {
        Ok(self.sftp.rename(src, dst, None)?)
    }
    fn stat(&self, path: &Path) -> Result<FileStatus> {
        let stat = self.sftp.lstat(path)?;
        Ok(FileStatus::try_from(&stat)?)
    }

}

impl Vfs for LocalVfs {
    fn open_dir(&mut self, path: &Path) -> Result<Box<dyn VfsFile>> {
        let r = std::fs::read_dir(&path).context(line!())?;
        Ok(Box::new(LocalFile { path: path.to_path_buf(), itr: r }))
    }
    fn base_dir(&self) -> &PathBuf {
        &self.base_dir
    }

    fn open(&self, filename: &Path) -> Result<Box<dyn Read>> {
        let r = std::fs::File::open(&filename)?;
        Ok(Box::new(r))
    }
    fn create(&self, filename: &Path) -> Result<Box<dyn Write>> {
        let r = std::fs::File::create(&filename)?;
        Ok(Box::new(r))
    }
    fn set_perm(&self, path: &Path) -> Result<()> {
        Ok(())
    }
    fn rename(&self, src: &Path, dst: &Path) -> Result<()> {
        Ok(std::fs::rename(src, dst)?)
    }
    fn stat(&self, path: &Path) -> Result<FileStatus> {
        let md = std::fs::metadata(&path)?;
        Ok(FileStatus::try_from(&md)?)
    }

}

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

pub trait VfsFile {
    fn read_all_dir_entry(&mut self)
                          -> Result<Vec<(PathBuf, Option<FileStatus>)>>;
}


impl VfsFile for SftpFile {
    fn read_all_dir_entry(&mut self) -> Result<Vec<(PathBuf, Option<FileStatus>)>>
    {
        let mut list = vec![];
        let (this_dir, par_dir) = (Path::new("."), Path::new(".."));
        loop {
            match self.file.readdir() {
                Ok((filename, stat)) => {
                    if &*filename == this_dir || &*filename == par_dir {
                        continue;
                    }

                    let status = FileStatus::try_from(&stat).context("next_dir_entry of SftpFile canon")?;
                    trace!("next_dir_entry sftp return: {}", filename.display());
                    list.push( (filename, Some(status)) );
                }
                Err(ref e) if e.code() == LIBSSH2_ERROR_FILE => return Ok(list),
                Err(e) => return Err(ERR!("error on next readdir: {}", e)),
            };
        }

    }
}

impl VfsFile for LocalFile {
    fn read_all_dir_entry(&mut self) -> Result<Vec<(PathBuf, Option<FileStatus>)>>
    {
        let mut list = vec![];
        loop {
            match self.itr.next() {
                None => return Ok(list),
                Some(r) => match r {
                    Err(e) => return Err(ERR!("error on reading next entry in ReadDir: {}", e)),
                    Ok(de) => {
                        list.push( (de.path(), None) );
                    },
                }
            }
        }
    }
}

