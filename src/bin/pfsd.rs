use core::hash;
use std::{
    collections::BTreeMap,
    env,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

use base64::Engine;
use chrono::{DateTime, Utc};
use fuser::{FileAttr, Filesystem};
use libc::{O_RDWR, O_WRONLY};
use log::{debug, error, info, warn};
use serde::Serialize;

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Clone, Copy)]
struct FsNodeHash([u8; 32]);
#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Clone, Copy)]
struct ContentHash([u8; 32]);

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Clone, Copy)]
struct InodeNumber(usize);

fn calculate_hash<T: Serialize>(v: &T) -> [u8; 32] {
    let mut buffer = Vec::new();
    ciborium::into_writer(v, &mut buffer).expect("To be able to serialize");
    hmac_sha256::Hash::hash(&buffer)
}

#[derive(Default, Clone, Copy, Serialize)]
enum FileType {
    #[default]
    Directory,
    RegularFile,
    Symlink,
}

#[derive(Default, Clone, Copy, Serialize)]
struct FsNode {
    /// (recursive?) size in bytes
    size: u64,
    /// Time of last change to the content
    modification_time: DateTime<Utc>,
    /// The hash of the content, SHA256 probably
    content_hash: ContentHash,
    /// Kind of file
    kind: FileType,
}

impl FsNode {
    fn as_file_attr(&self, inode_number: InodeNumber) -> FileAttr {
        let is_directory = if let FileType::Directory = self.kind {
            true
        } else {
            false
        };
        FileAttr {
            ino: inode_number.0 as u64,
            size: self.size,
            blocks: 0,
            atime: SystemTime::now(),
            mtime: SystemTime::from(self.modification_time),
            ctime: SystemTime::now(),
            crtime: SystemTime::now(),
            kind: if !is_directory {
                fuser::FileType::RegularFile
            } else {
                fuser::FileType::Directory
            },
            perm: 0o755,
            nlink: if !is_directory { 1 } else { 2 },
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
            blksize: 0,
        }
    }
}

#[derive(Default, Serialize, Clone)]
struct DirectoryEntry {
    name: String,
    fs_node_hash: FsNodeHash,
    #[serde(skip_serializing)]
    inode_number: InodeNumber,
}

#[derive(Default, Serialize, Clone)]
struct Directory {
    entries: Vec<DirectoryEntry>,
    #[serde(skip_serializing)]
    parent_node_hash: Option<FsNodeHash>,
}

struct OpenFile {
    path: PathBuf,
    backing_fs_node_hash: FsNodeHash,
    backing_file: File,
    writable: bool,
}

#[derive(Default)]
struct Pfs {
    root_node: FsNode,
    fs_nodes: BTreeMap<FsNodeHash, FsNode>,
    directories: BTreeMap<ContentHash, Directory>,
    data_dir: String,
    inodes: Vec<FsNodeHash>,
    open_files: Vec<Option<OpenFile>>,
}

impl Pfs {
    fn find_file(p: &Path) -> Option<FsNode> {
        for segment in p {}
        None
    }

    fn empty(data_dir: String) -> Pfs {
        let mut pfs = Pfs {
            data_dir,
            root_node: FsNode::default(),
            directories: BTreeMap::new(),
            inodes: vec![FsNodeHash::default(); 1],
            ..Default::default()
        };
        let (fs_content_hash, content_hash, inode_number, fs_node) =
            pfs.new_directory_node(Directory::default());
        pfs.root_node = fs_node;
        pfs
    }

    fn new_directory_node(
        &mut self,
        directory: Directory,
    ) -> (FsNodeHash, ContentHash, InodeNumber, FsNode) {
        let content_hash = ContentHash(calculate_hash(&directory));
        let directory_node = FsNode {
            kind: FileType::Directory,
            modification_time: Utc::now(),
            size: 0,
            content_hash,
        };
        let fs_node_hash = FsNodeHash(calculate_hash(&directory_node));
        self.fs_nodes.insert(fs_node_hash, directory_node);
        self.inodes.push(fs_node_hash);
        let inode_number = InodeNumber(self.inodes.len() - 1);
        self.directories.insert(content_hash, directory);
        (fs_node_hash, content_hash, inode_number, directory_node)
    }

    fn add_directory_entry(
        &mut self,
        fs_node: &FsNode,
        new_entry: DirectoryEntry,
    ) -> (FsNodeHash, ContentHash, InodeNumber, FsNode) {
        let mut directory = self.directories.get(&fs_node.content_hash).unwrap().clone();
        directory.entries.push(new_entry);
        let (fs_node_hash, content_hash, inode_number, fs_node) =
            self.new_directory_node(directory.clone());
        if let Some(parent_hash) = directory.parent_node_hash {
            let (new_parent_node_hash, _, _, _) = self.update_directory_entry(
                &parent_hash,
                &FsNodeHash(calculate_hash(&fs_node)),
                fs_node_hash,
                inode_number,
            );
            self.directories
                .get_mut(&content_hash)
                .unwrap()
                .parent_node_hash = Some(new_parent_node_hash);
        } else {
            self.root_node = fs_node;
        }
        (fs_node_hash, content_hash, inode_number, fs_node)
    }

    fn update_directory_entry(
        &mut self,
        old_directory_hash: &FsNodeHash,
        old_entry_hash: &FsNodeHash,
        new_entry_fs_node_hash: FsNodeHash,
        new_entry_inode_number: InodeNumber,
    ) -> (FsNodeHash, ContentHash, InodeNumber, FsNode) {
        let directory_node = self.fs_nodes.get(old_directory_hash).unwrap();
        let mut directory = self
            .directories
            .get(&directory_node.content_hash)
            .unwrap()
            .clone();
        for entry in directory.entries.iter_mut() {
            if &entry.fs_node_hash == old_entry_hash {
                entry.fs_node_hash = new_entry_fs_node_hash;
                entry.inode_number = new_entry_inode_number;
            }
        }
        let old_parent_node_hash = directory.parent_node_hash;
        let (fs_node_hash, content_hash, inode_number, fs_node) =
            self.new_directory_node(directory);
        if let Some(old_parent_node_hash) = old_parent_node_hash {
            let (new_parent_node_hash, _, _, _) = self.update_directory_entry(
                &old_parent_node_hash,
                old_directory_hash,
                fs_node_hash,
                inode_number,
            );
            self.directories
                .get_mut(&content_hash)
                .unwrap()
                .parent_node_hash = Some(new_parent_node_hash);
        } else {
            self.root_node = fs_node;
        }
        (fs_node_hash, content_hash, inode_number, fs_node)
    }

    fn new_file_node(
        &mut self,
        content_hash: ContentHash,
        size: u64,
    ) -> (FsNodeHash, InodeNumber, FsNode) {
        let file_node = FsNode {
            kind: FileType::RegularFile,
            modification_time: Utc::now(),
            size,
            content_hash,
        };
        let fs_node_hash = FsNodeHash(calculate_hash(&file_node));
        self.fs_nodes.insert(fs_node_hash, file_node);
        self.inodes.push(fs_node_hash);
        (fs_node_hash, InodeNumber(self.inodes.len() - 1), file_node)
    }
}

impl Filesystem for Pfs {
    fn getattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: Option<u64>,
        reply: fuser::ReplyAttr,
    ) {
        debug!("Called getattr with ino: {} fh: {:?}", ino, fh);
        let ttl = Duration::from_millis(1);
        let fs_node_hash = self.inodes.get(ino as usize).unwrap();
        let fs_node = self.fs_nodes.get(fs_node_hash).unwrap();
        let attrs = fs_node.as_file_attr(InodeNumber(ino as usize));
        debug!("Attrs are {:?}", attrs);
        reply.attr(&ttl, &attrs);
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        debug!(
            "Called readdir with ino: {} fh: {} offset: {}",
            ino, fh, offset
        );
        if ino == 1 {
            if offset == 0 {
                reply.add(1, 0, fuser::FileType::Directory, ".");
                reply.add(1, 1, fuser::FileType::Directory, "..");
                let root_dir = self
                    .directories
                    .get(&self.root_node.content_hash)
                    .expect("To get root dir");
                let mut offset = 2;
                for entry in root_dir.entries.iter() {
                    info!(
                        "Dir entry with name {} and ino {}",
                        &entry.name, entry.inode_number.0
                    );
                    reply.add(
                        entry.inode_number.0 as u64,
                        offset,
                        fuser::FileType::RegularFile,
                        &entry.name,
                    );
                    offset += 1;
                }
            }
            reply.ok()
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn mkdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        let fs_node_hash = *self.inodes.get(parent as usize).unwrap();
        let fs_node = *self.fs_nodes.get(&fs_node_hash).unwrap();
        if let FileType::Directory = fs_node.kind {
        } else {
            return reply.error(libc::ENOSYS);
        }
        let (fs_node_hash, content_hash, inode_number, new_fs_node) =
            self.new_directory_node(Directory::default());
        self.add_directory_entry(
            &fs_node,
            DirectoryEntry {
                name: name.to_str().unwrap().to_owned(),
                fs_node_hash,
                inode_number,
            },
        );
        reply.entry(
            &Duration::from_millis(1),
            &new_fs_node.as_file_attr(inode_number),
            0,
        );
    }

    fn create(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        debug!(
            "Called create with parent: {} name: {:?} mode: {} umask: {} flags: {}",
            parent, name, mode, umask, flags
        );
        if parent != fuser::FUSE_ROOT_ID {
            return reply.error(libc::ENOSYS);
        }
        let temp_file_path = self.data_dir.clone() + "/" + &Utc::now().to_rfc3339();
        let (fs_node_hash, inode_number, fs_node) = self.new_file_node(ContentHash::default(), 0);
        let open_file = OpenFile {
            backing_file: File::create_new(&temp_file_path).unwrap(),
            backing_fs_node_hash: fs_node_hash,
            path: PathBuf::from(temp_file_path),
            writable: (flags & O_WRONLY > 0) || (flags & O_RDWR > 0),
        };
        self.open_files.push(Some(open_file));
        let root_node = self.root_node;
        let (_root_node_hash, _root_dir_hash, _root_inode_number, _root_node) = self
            .add_directory_entry(
                &root_node,
                DirectoryEntry {
                    name: name.to_str().unwrap().to_owned(),
                    fs_node_hash,
                    inode_number,
                },
            );
        reply.created(
            &Duration::from_secs(60),
            &fs_node.as_file_attr(inode_number),
            0,
            (self.open_files.len() - 1).try_into().unwrap(),
            0,
        );
    }

    fn open(&mut self, _req: &fuser::Request<'_>, _ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        let writable = _flags & libc::O_WRONLY > 0 || _flags & libc::O_RDWR > 0;
        if writable {
            warn!("Tried to open a file in write mode");
            return reply.error(libc::ENOSYS);
        };
        let fs_node_hash = self.inodes.get(_ino as usize).unwrap();
        let content_hash = self.fs_nodes.get(fs_node_hash).unwrap().content_hash;
        let path = PathBuf::from(
            self.data_dir.clone()
                + "/"
                + &base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(content_hash.0),
        );
        let open_file = OpenFile {
            backing_file: File::open(&path).unwrap(),
            backing_fs_node_hash: *fs_node_hash,
            path,
            writable,
        };
        self.open_files.push(Some(open_file));
        let fh = (self.open_files.len() - 1).try_into().unwrap();
        reply.opened(fh, 0);
    }

    fn write(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        write_flags: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        debug!(
            "Called write with ino: {} fh: {} offset: {} write_flags: {} flags: {} lock_owner: {:?}",
            ino, fh, offset, write_flags, flags, lock_owner
        );
        let open_file = self.open_files[fh as usize].as_mut().unwrap();
        open_file.backing_file.write_at(data, offset as u64);
        reply.written(data.len() as u32);
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let Some(Some(open_file)) = self.open_files.get_mut(fh as usize) else {
            return reply.error(libc::ENOENT);
        };
        open_file.backing_file.seek(SeekFrom::Current(offset));
        let mut buf = vec![0u8; size as usize];
        open_file.backing_file.read(&mut buf);
        reply.data(&buf);
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
        let mut open_file = std::mem::replace(&mut self.open_files[fh as usize], None).unwrap();
        if !open_file.writable {
            return reply.ok();
        };
        let fd = &mut open_file.backing_file;
        fd.flush().expect("Flush to succeed");
        fd.seek(std::io::SeekFrom::Start(0))
            .expect("Seek to succeed");
        let mut size = 0u64;
        let mut buf = [0u8; 1024];
        let mut hasher = hmac_sha256::Hash::new();
        while let Ok(n) = fd.read(&mut buf) {
            if n == 0 {
                break;
            };
            debug!("Read {} bytes to feed to hasher", n);
            hasher.update(&buf[..n]);
            size += n as u64;
        }
        drop(fd);
        let content_hash = ContentHash(hasher.finalize());
        let new_file_path = self.data_dir.clone()
            + "/"
            + &base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(content_hash.0);
        if let Err(e) = std::fs::rename(&open_file.path, &new_file_path) {
            error!(
                "Unable to move closed file from {:?} to {} because of {}",
                open_file.path, new_file_path, e
            );
        };
        let original_fs_node_hash = open_file.backing_fs_node_hash;
        debug!(
            "Storing new fs node with content hash {:?} and size {}",
            content_hash, size
        );
        let (fs_node_hash, inode_number, fs_node) = self.new_file_node(content_hash, size);
        let root_node = self.root_node;
        self.update_directory_entry(
            &FsNodeHash(calculate_hash(&root_node)),
            &original_fs_node_hash,
            fs_node_hash,
            inode_number,
        );
        self.inodes[_ino as usize] = fs_node_hash;
        reply.ok()
    }

    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        if parent != 1 {
            return reply.error(libc::ENOENT);
        }
        let root_directory = self.directories.get(&self.root_node.content_hash).unwrap();
        for entry in root_directory.entries.iter() {
            if &entry.name == name.to_str().unwrap() {
                let fs_node = self.fs_nodes.get(&entry.fs_node_hash).unwrap();
                return reply.entry(
                    &Duration::from_millis(1),
                    &fs_node.as_file_attr(entry.inode_number),
                    0,
                );
            }
        }
        return reply.error(libc::ENOENT);
    }
}

fn main() {
    simple_logger::init().unwrap();
    let Ok(data_home) = env::var("XDG_DATA_HOME").or(env::var("HOME").map(|h| h + "/.local/share"))
    else {
        println!("Either $XDG_DATA_HOME or $HOME must be set");
        return;
    };
    let mount_point = data_home.clone() + "/pfs";
    let data_dir = data_home + "/pfs_data";
    std::fs::create_dir_all(&data_dir).expect("To create the data dir");
    std::fs::create_dir_all(data_dir.clone() + "/tmp").expect("To create the temporary file dir");
    let fs = Pfs::empty(data_dir);
    info!("Mounting to {}", mount_point);
    let (send, recv) = std::sync::mpsc::channel();
    let send_ctrlc = send.clone();
    ctrlc::set_handler(move || {
        send_ctrlc.send(()).unwrap();
    })
    .unwrap();
    let guard = fuser::spawn_mount2(fs, &mount_point, &vec![]).unwrap();
    let () = recv.recv().unwrap();
    drop(guard)
}
