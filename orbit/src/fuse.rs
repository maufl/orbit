use std::io::Write;
use std::time::{Duration, SystemTime};

use fuser::{FileAttr, Filesystem};
use log::warn;

use crate::{InodeNumber, OrbitFs, RuntimeDirectoryEntryInfo, RuntimeFsNode};

pub fn as_file_attr(fs_node: &RuntimeFsNode, inode_number: InodeNumber) -> FileAttr {
    let is_directory = fs_node.is_directory();
    FileAttr {
        ino: inode_number.0 as u64,
        size: fs_node.size,
        blocks: 0,
        atime: SystemTime::now(),
        mtime: SystemTime::from(fs_node.modification_time),
        ctime: SystemTime::now(),
        crtime: SystemTime::now(),
        kind: if !is_directory {
            fuser::FileType::RegularFile
        } else {
            fuser::FileType::Directory
        },
        perm: 0o755,
        nlink: if !is_directory { 1 } else { 2 },
        uid: nix::unistd::getuid().as_raw(),
        gid: nix::unistd::getgid().as_raw(),
        rdev: 0,
        flags: 0,
        blksize: 0,
    }
}

pub fn getattr(orbit_fs: &OrbitFs, ino: u64) -> Result<FileAttr, libc::c_int> {
    let runtime_data = orbit_fs.runtime_data.read();
    let Some(fs_node) = runtime_data.inodes.get(ino as usize) else {
        return Err(libc::ENOENT);
    };
    let attrs = as_file_attr(fs_node, InodeNumber(ino));
    Ok(attrs)
}

pub fn readdir(
    orbit_fs: &OrbitFs,
    ino: u64,
    offset: i64,
) -> Result<Vec<RuntimeDirectoryEntryInfo>, libc::c_int> {
    let (fs_node, directory) = orbit_fs.get_directory(ino)?;
    let mut entries = Vec::new();

    if offset == 0 {
        entries.push(RuntimeDirectoryEntryInfo {
            ino,
            offset: 1,
            file_type: fuser::FileType::Directory,
            name: ".".to_string(),
        });
        entries.push(RuntimeDirectoryEntryInfo {
            ino: fs_node.parent_inode_number.map(|i| i.0).unwrap_or(ino),
            offset: 2,
            file_type: fuser::FileType::Directory,
            name: "..".to_string(),
        });

        let runtime_data = orbit_fs.runtime_data.read();
        let inodes = &runtime_data.inodes;
        let mut entry_offset = 3;
        for entry in directory.entries.iter() {
            let fs_node = inodes[entry.inode_number.0 as usize];
            entries.push(RuntimeDirectoryEntryInfo {
                ino: entry.inode_number.0 as u64,
                offset: entry_offset,
                file_type: if fs_node.is_directory() {
                    fuser::FileType::Directory
                } else {
                    fuser::FileType::RegularFile
                },
                name: entry.name.clone(),
            });
            entry_offset += 1;
        }
    }
    Ok(entries)
}

pub fn mkdir(orbit_fs: &mut OrbitFs, parent: u64, name: &str) -> Result<FileAttr, libc::c_int> {
    match orbit_fs.get_directory(parent) {
        Ok(_) => {}
        Err(e) => return Err(e),
    };
    let fs_node = orbit_fs.new_directory_node(
        crate::RuntimeDirectory::default(),
        Some(InodeNumber(parent)),
    );
    let inode_number = orbit_fs.assign_inode_number(fs_node);
    orbit_fs.add_directory_entry(
        InodeNumber(parent),
        crate::RuntimeDirectoryEntry {
            name: name.to_owned(),
            fs_node_hash: fs_node.calculate_hash(),
            inode_number,
        },
    )?;
    Ok(as_file_attr(&fs_node, inode_number))
}

pub fn create(
    orbit_fs: &mut OrbitFs,
    parent: u64,
    name: &str,
    flags: i32,
) -> Result<(FileAttr, u64), libc::c_int> {
    use crate::ContentHash;
    use chrono::Utc;
    use libc::{O_RDWR, O_WRONLY};
    use std::fs::File;

    let temp_file_path = orbit_fs.data_dir.clone() + "/" + &Utc::now().to_rfc3339();
    let fs_node = orbit_fs.new_file_node_with_persistence(
        ContentHash::default(),
        0,
        Some(InodeNumber(parent)),
    );

    let inode_number = orbit_fs.assign_inode_number(fs_node);
    let open_file = crate::OpenFile {
        backing_file: File::create_new(&temp_file_path).map_err(|_| libc::EIO)?,
        parent_inode_number: InodeNumber(parent),
        path: std::path::PathBuf::from(temp_file_path),
        writable: (flags & O_WRONLY > 0) || (flags & O_RDWR > 0),
    };
    orbit_fs
        .runtime_data
        .write()
        .open_files
        .push(Some(open_file));
    orbit_fs.add_directory_entry(
        InodeNumber(parent),
        crate::RuntimeDirectoryEntry {
            name: name.to_owned(),
            fs_node_hash: fs_node.calculate_hash(),
            inode_number,
        },
    )?;
    let fh = (orbit_fs.runtime_data.read().open_files.len() - 1) as u64;
    Ok((as_file_attr(&fs_node, inode_number), fh))
}

pub fn open(orbit_fs: &mut OrbitFs, ino: u64, flags: i32) -> Result<u64, libc::c_int> {
    use base64::Engine;
    use std::fs::File;
    use std::path::PathBuf;

    let writable = flags & libc::O_WRONLY > 0 || flags & libc::O_RDWR > 0;
    if writable {
        return Err(libc::ENOSYS);
    }
    let fs_node = orbit_fs.runtime_data.read().inodes[ino as usize];
    let content_hash = fs_node.content_hash;
    let path = PathBuf::from(
        orbit_fs.data_dir.clone()
            + "/"
            + &base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(content_hash.0),
    );
    let backing_file = File::open(&path).map_err(|_| libc::ENOENT)?;
    let open_file = crate::OpenFile {
        backing_file,
        parent_inode_number: fs_node.parent_inode_number.unwrap(),
        path,
        writable,
    };
    let mut runtime_data = orbit_fs.runtime_data.write();
    runtime_data.open_files.push(Some(open_file));
    let fh = (runtime_data.open_files.len() - 1) as u64;
    Ok(fh)
}

pub fn write(
    orbit_fs: &mut OrbitFs,
    _ino: u64,
    fh: u64,
    offset: i64,
    data: &[u8],
) -> Result<u32, libc::c_int> {
    use std::os::unix::fs::FileExt;

    let mut runtime_data = orbit_fs.runtime_data.write();
    let open_file = runtime_data
        .open_files
        .get_mut(fh as usize)
        .and_then(|opt| opt.as_mut())
        .ok_or(libc::ENOENT)?;
    open_file
        .backing_file
        .write_at(data, offset as u64)
        .map_err(|_| libc::EIO)?;
    Ok(data.len() as u32)
}

pub fn read(
    orbit_fs: &mut OrbitFs,
    _ino: u64,
    fh: u64,
    offset: i64,
    size: u32,
) -> Result<Vec<u8>, libc::c_int> {
    use std::io::{Read, Seek, SeekFrom};

    let mut runtime_data = orbit_fs.runtime_data.write();
    let open_file = runtime_data
        .open_files
        .get_mut(fh as usize)
        .and_then(|opt| opt.as_mut())
        .ok_or(libc::ENOENT)?;
    open_file
        .backing_file
        .seek(SeekFrom::Start(offset as u64))
        .map_err(|_| libc::EIO)?;
    let mut buf = vec![0u8; size as usize];
    let bytes_read = open_file
        .backing_file
        .read(&mut buf)
        .map_err(|_| libc::EIO)?;
    buf.truncate(bytes_read);
    Ok(buf)
}

pub fn release(orbit_fs: &mut OrbitFs, ino: u64, fh: u64) -> Result<(), libc::c_int> {
    use crate::ContentHash;
    use base64::Engine;
    use std::io::{Read, Seek};

    let mut open_file = {
        let mut runtime_data = orbit_fs.runtime_data.write();
        std::mem::replace(&mut runtime_data.open_files[fh as usize], None).ok_or(libc::ENOENT)?
    };
    if !open_file.writable {
        return Ok(());
    }
    let fd = &mut open_file.backing_file;
    fd.flush().map_err(|_| libc::EIO)?;
    fd.seek(std::io::SeekFrom::Start(0))
        .map_err(|_| libc::EIO)?;
    let mut size = 0u64;
    let mut buf = [0u8; 1024];
    let mut hasher = hmac_sha256::Hash::new();
    while let Ok(n) = fd.read(&mut buf) {
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        size += n as u64;
    }
    let content_hash = ContentHash(hasher.finalize());
    let new_file_path = orbit_fs.data_dir.clone()
        + "/"
        + &base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(content_hash.0);
    std::fs::rename(&open_file.path, &new_file_path).map_err(|_| libc::EIO)?;
    let old_fs_node = orbit_fs.runtime_data.read().inodes[ino as usize];
    let new_fs_node =
        orbit_fs.new_file_node_with_persistence(content_hash, size, Some(InodeNumber(ino)));
    orbit_fs.update_directory_entry(
        open_file.parent_inode_number,
        &old_fs_node.calculate_hash(),
        new_fs_node.calculate_hash(),
    )?;
    orbit_fs.runtime_data.write().inodes[ino as usize] = new_fs_node;
    Ok(())
}

pub fn lookup(orbit_fs: &OrbitFs, parent: u64, name: &str) -> Result<FileAttr, libc::c_int> {
    let (_fs_node, directory) = orbit_fs.get_directory(parent)?;
    let runtime_data = orbit_fs.runtime_data.read();
    for entry in directory.entries.iter() {
        if entry.name == name {
            let fs_node = &runtime_data.inodes[entry.inode_number.0 as usize];
            return Ok(as_file_attr(fs_node, entry.inode_number));
        }
    }
    Err(libc::ENOENT)
}

pub fn unlink(orbit_fs: &mut OrbitFs, parent: u64, name: &str) -> Result<(), libc::c_int> {
    use crate::FileType;

    let (_fs_node, directory) = orbit_fs.get_directory(parent)?;
    let file_entry = directory
        .entries
        .iter()
        .find(|entry| entry.name == name)
        .ok_or(libc::ENOENT)?;
    let file_node_kind = {
        let runtime_data = orbit_fs.runtime_data.read();
        runtime_data
            .inodes
            .get(file_entry.inode_number.0 as usize)
            .map(|node| node.kind)
            .ok_or(libc::ENOENT)?
    };
    if file_node_kind != FileType::RegularFile {
        return Err(libc::EISDIR);
    }
    orbit_fs.remove_directory_entry(InodeNumber(parent), name)
}

pub fn rename(
    orbit_fs: &mut OrbitFs,
    parent: u64,
    name: &str,
    newparent: u64,
    newname: &str,
    no_replace: bool,
) -> Result<(), libc::c_int> {
    use crate::RuntimeDirectoryEntry;

    if parent == newparent && name == newname {
        return Ok(());
    }
    let (_source_fs_node, source_directory) = orbit_fs.get_directory(parent)?;
    let source_entry = source_directory
        .entries
        .iter()
        .find(|entry| entry.name == name)
        .ok_or(libc::ENOENT)?
        .clone();
    let (_dest_fs_node, dest_directory) = orbit_fs.get_directory(newparent)?;
    let dest_exists = dest_directory
        .entries
        .iter()
        .any(|entry| entry.name == newname);

    if no_replace && dest_exists {
        return Err(libc::EEXIST);
    }

    // If destination exists and we're allowed to replace, remove it first
    if !no_replace && dest_exists {
        orbit_fs.remove_directory_entry(InodeNumber(newparent), newname)?;
    }

    if parent != newparent {
        let mut runtime_data = orbit_fs.runtime_data.write();
        let fs_node = runtime_data
            .inodes
            .get_mut(source_entry.inode_number.0 as usize)
            .ok_or(libc::ENOENT)?;
        fs_node.parent_inode_number = Some(InodeNumber(newparent));
    }
    let new_entry = RuntimeDirectoryEntry {
        name: newname.to_owned(),
        fs_node_hash: source_entry.fs_node_hash,
        inode_number: source_entry.inode_number,
    };
    orbit_fs.add_directory_entry(InodeNumber(newparent), new_entry)?;
    orbit_fs.remove_directory_entry(InodeNumber(parent), name)?;
    Ok(())
}

impl Filesystem for OrbitFs {
    fn getattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: Option<u64>,
        reply: fuser::ReplyAttr,
    ) {
        let ttl = Duration::from_millis(1);

        match getattr(self, ino) {
            Ok(attrs) => reply.attr(&ttl, &attrs),
            Err(error) => {
                warn!("getattr failed for ino {}: {}", ino, error);
                reply.error(error);
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        match readdir(self, ino, offset) {
            Ok(entries) => {
                for entry in entries {
                    let _ = reply.add(entry.ino, entry.offset, entry.file_type, &entry.name);
                }
                reply.ok();
            }
            Err(error) => {
                warn!("readdir failed for ino {}: {}", ino, error);
                reply.error(error);
            }
        }
    }

    fn mkdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        _mode: u32,
        _umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        let name_str = match name.to_str() {
            Some(n) => n,
            None => return reply.error(libc::EINVAL),
        };

        match mkdir(self, parent, name_str) {
            Ok(attrs) => reply.entry(&Duration::from_millis(1), &attrs, 0),
            Err(error) => {
                warn!(
                    "mkdir failed for parent {} name {}: {}",
                    parent, name_str, error
                );
                reply.error(error);
            }
        }
    }

    fn create(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        _mode: u32,
        _umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let name_str = match name.to_str() {
            Some(n) => n,
            None => return reply.error(libc::EINVAL),
        };

        match create(self, parent, name_str, flags) {
            Ok((attrs, fh)) => reply.created(&Duration::from_secs(60), &attrs, 0, fh, 0),
            Err(error) => {
                warn!(
                    "create failed for parent {} name {}: {}",
                    parent, name_str, error
                );
                reply.error(error);
            }
        }
    }

    fn open(&mut self, _req: &fuser::Request<'_>, _ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        match open(self, _ino, _flags) {
            Ok(fh) => reply.opened(fh, 0),
            Err(error) => {
                warn!("open failed for ino {}: {}", _ino, error);
                if error == libc::ENOENT {
                    if let Some(ref net) = self.network_communication {
                        let content_hash =
                            self.runtime_data.read().inodes[_ino as usize].content_hash;
                        log::debug!(
                            "File {} is not present locally, requesting it from peers",
                            content_hash
                        );
                        let mut orbit_fs = self.clone();
                        net.request_file_with_callback(
                            content_hash,
                            Duration::from_secs(10),
                            Box::new(move || {
                                log::debug!("It seems like we received the file {} from a peer, try opening it again", content_hash);
                                match open(&mut orbit_fs, _ino, _flags) {
                                Ok(fh) => reply.opened(fh, 0),
                                Err(error) => {
                                    warn!("Failed to request file {} from peers", content_hash);
                                    reply.error(error);
                                }
                                }
                            }),
                        );
                        return;
                    }
                }
                reply.error(error);
            }
        }
    }

    fn write(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        match write(self, _ino, fh, offset, data) {
            Ok(bytes_written) => reply.written(bytes_written),
            Err(error) => {
                warn!("write failed for ino {} fh {}: {}", _ino, fh, error);
                reply.error(error);
            }
        }
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        match read(self, _ino, fh, offset, size) {
            Ok(data) => reply.data(&data),
            Err(error) => {
                warn!("read failed for ino {} fh {}: {}", _ino, fh, error);
                reply.error(error);
            }
        }
    }

    fn release(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        match release(self, _ino, fh) {
            Ok(()) => reply.ok(),
            Err(error) => {
                warn!("release failed for ino {} fh {}: {}", _ino, fh, error);
                reply.error(error);
            }
        }
    }

    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let name_str = match name.to_str() {
            Some(n) => n,
            None => return reply.error(libc::EINVAL),
        };

        match lookup(self, parent, name_str) {
            Ok(attrs) => reply.entry(&Duration::from_millis(1), &attrs, 0),
            Err(error) => {
                warn!(
                    "lookup failed for parent {} name {}: {}",
                    parent, name_str, error
                );
                reply.error(error);
            }
        }
    }

    fn unlink(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let file_name = match name.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EINVAL),
        };

        match unlink(self, parent, file_name) {
            Ok(()) => reply.ok(),
            Err(error) => {
                warn!(
                    "unlink failed for parent {} name {}: {}",
                    parent, file_name, error
                );
                reply.error(error);
            }
        }
    }

    fn rename(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        newparent: u64,
        newname: &std::ffi::OsStr,
        flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        let old_name = match name.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EINVAL),
        };

        let new_name = match newname.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EINVAL),
        };

        let no_replace = flags & libc::RENAME_NOREPLACE > 0;

        match rename(self, parent, old_name, newparent, new_name, no_replace) {
            Ok(()) => reply.ok(),
            Err(error) => {
                warn!(
                    "rename failed for parent {} name {} to newparent {} newname {}: {}",
                    parent, old_name, newparent, new_name, error
                );
                reply.error(error);
            }
        }
    }
}
