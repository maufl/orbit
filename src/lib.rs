use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::FileExt,
    path::PathBuf,
    time::{Duration, SystemTime},
};

use base64::Engine;
use chrono::{DateTime, Utc};
use fjall::{Config, Keyspace, PartitionHandle};
use fuser::{FileAttr, Filesystem};
use libc::{O_RDWR, O_WRONLY};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Clone, Copy)]
struct ContentHash([u8; 32]);

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Clone, Copy)]
struct FsNodeHash([u8; 32]);

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Clone, Copy)]
struct InodeNumber(u64);

fn calculate_hash<T: Serialize>(v: &T) -> [u8; 32] {
    let mut buffer = Vec::new();
    ciborium::into_writer(v, &mut buffer).expect("To be able to serialize");
    hmac_sha256::Hash::hash(&buffer)
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq)]
enum FileType {
    #[default]
    Directory,
    RegularFile,
    Symlink,
}

#[derive(Default, Clone, Copy, Serialize, Deserialize)]
struct FsNode {
    /// (recursive?) size in bytes
    size: u64,
    /// Time of last change to the content
    modification_time: DateTime<Utc>,
    /// The hash of the content, SHA256 probably
    content_hash: ContentHash,
    /// Kind of file
    kind: FileType,
    #[serde(skip_serializing, skip_deserializing)]
    parent_inode_number: Option<InodeNumber>,
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
            uid: nix::unistd::getuid().as_raw(),
            gid: nix::unistd::getgid().as_raw(),
            rdev: 0,
            flags: 0,
            blksize: 0,
        }
    }

    fn new_directory_node(
        content_hash: ContentHash,
        size: u64,
        parent_inode_number: Option<InodeNumber>,
    ) -> FsNode {
        FsNode {
            kind: FileType::Directory,
            modification_time: Utc::now(),
            size,
            content_hash,
            parent_inode_number,
        }
    }

    fn new_file_node(
        content_hash: ContentHash,
        size: u64,
        parent_inode_number: Option<InodeNumber>,
    ) -> FsNode {
        FsNode {
            kind: FileType::RegularFile,
            modification_time: Utc::now(),
            size,
            content_hash,
            parent_inode_number,
        }
    }

    fn calculate_hash(&self) -> FsNodeHash {
        FsNodeHash(calculate_hash(&self))
    }
}

#[derive(Default, Serialize, Deserialize, Clone)]
struct DirectoryEntry {
    name: String,
    fs_node_hash: FsNodeHash,
    #[serde(skip_serializing, skip_deserializing)]
    inode_number: InodeNumber,
}

#[derive(Default, Serialize, Deserialize, Clone)]
struct Directory {
    entries: Vec<DirectoryEntry>,
    #[serde(skip_serializing, skip_deserializing)]
    parent_inode_number: Option<InodeNumber>,
}

impl Directory {
    fn calculate_hash(&self) -> ContentHash {
        ContentHash(calculate_hash(&self))
    }
}

struct OpenFile {
    path: PathBuf,
    backing_file: File,
    parent_inode_number: InodeNumber,
    writable: bool,
}

pub struct Pfs {
    directories: BTreeMap<ContentHash, Directory>,
    data_dir: String,
    inodes: Vec<FsNode>,
    open_files: Vec<Option<OpenFile>>,
    /// This field may not be removed! It looks unused, but if you drop the Keyspace fjall will stop working
    keyspace: Keyspace,
    fs_nodes_partition: PartitionHandle,
    directories_partition: PartitionHandle,
}

impl Pfs {
    pub fn initialize(data_dir: String) -> Result<Pfs, Box<dyn std::error::Error>> {
        let kv_path = format!("{}/metadata", data_dir);
        std::fs::create_dir_all(&kv_path)?;

        let keyspace = Config::new(&kv_path).open()?;
        let fs_nodes_partition = keyspace.open_partition("fs_nodes", Default::default())?;
        let directories_partition = keyspace.open_partition("directories", Default::default())?;

        let mut pfs = Pfs {
            data_dir: data_dir.clone(),
            directories: BTreeMap::new(),
            inodes: vec![FsNode::default(); 1],
            open_files: Vec::new(),
            keyspace,
            fs_nodes_partition,
            directories_partition,
        };

        // Try to load existing data
        if let Err(e) = pfs.restore_filesystem_tree() {
            warn!("Failed to load persisted data, starting fresh: {}", e);
            // Start fresh if loading fails
            pfs.directories.clear();
            pfs.inodes = vec![FsNode::default(); 1];
            let fs_node = pfs.new_directory_node(Directory::default(), None);
            pfs.assign_inode_number(fs_node.clone());
            if let Err(e) = pfs.persist_root_hash(&fs_node.calculate_hash()) {
                error!("Failed to persist root hash: {}", e);
            }
        }

        Ok(pfs)
    }

    fn load_fs_node(
        &self,
        node_hash: &FsNodeHash,
    ) -> Result<Option<FsNode>, Box<dyn std::error::Error>> {
        if let Some(value) = self.fs_nodes_partition.get(&node_hash.0)? {
            let fs_node: FsNode = ciborium::from_reader(&*value)?;
            return Ok(Some(fs_node));
        }
        Ok(None)
    }

    fn load_directory(
        &self,
        content_hash: &ContentHash,
    ) -> Result<Option<Directory>, Box<dyn std::error::Error>> {
        if let Some(value) = self.directories_partition.get(&content_hash.0)? {
            let directory: Directory = ciborium::from_reader(&*value)?;
            return Ok(Some(directory));
        }
        Ok(None)
    }

    fn restore_filesystem_tree(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Load the root hash first
        let key = b"__ROOT_HASH__";
        let Some(value) = self.fs_nodes_partition.get(key)? else {
            return Err("No root hash found in metadata".into());
        };
        let root_hash = ciborium::from_reader(&*value)?;
        info!("Found root hash {:?} in persistent storage", root_hash);

        // Restore the filesystem tree recursively, starting from the root
        let root_inode = self.restore_node_recursive(&root_hash, None)?;
        info!("Restored root node to inode {:?}", root_inode);

        info!(
            "Successfully restored filesystem tree from persistent storage with {} inodes",
            self.inodes.len()
        );
        Ok(())
    }

    fn restore_node_recursive(
        &mut self,
        fs_node_hash: &FsNodeHash,
        parent_inode_number: Option<InodeNumber>,
    ) -> Result<InodeNumber, Box<dyn std::error::Error>> {
        let Some(mut fs_node) = self.load_fs_node(fs_node_hash)? else {
            return Err(format!("Unable to load fs node with hash {:?}", fs_node_hash).into());
        };
        fs_node.parent_inode_number = parent_inode_number;
        let inode_number = self.assign_inode_number(fs_node);
        debug!(
            "Restored fs_node with hash {:?} to inode {:?}, type: {:?}",
            fs_node_hash, inode_number, fs_node.kind
        );
        if let FileType::Directory = fs_node.kind {
            // Load the directory structure
            if let Some(mut directory) = self.load_directory(&fs_node.content_hash)? {
                debug!("Loading directory with {} entries", directory.entries.len());
                // Update the parent inode number
                directory.parent_inode_number = parent_inode_number;

                // Restore each entry in the directory, updating with correct inode numbers
                for entry in &mut directory.entries {
                    debug!("Restoring directory entry: {}", entry.name);
                    let child_inode_number =
                        self.restore_node_recursive(&entry.fs_node_hash, Some(inode_number))?;
                    // Update the entry with the correct inode number
                    entry.inode_number = child_inode_number;
                }

                // Store the updated directory
                self.directories.insert(fs_node.content_hash, directory);
            } else {
                warn!(
                    "Directory content not found for FsNode with hash {:?}",
                    fs_node.content_hash
                );
            }
        }
        Ok(inode_number)
    }

    fn persist_fs_node(
        &self,
        node_hash: &FsNodeHash,
        fs_node: &FsNode,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = node_hash.0;
        let mut value = Vec::new();
        ciborium::into_writer(fs_node, &mut value)?;
        self.fs_nodes_partition.insert(&key, &value)?;
        debug!("Persisted FsNode with hash {:?}", node_hash);
        Ok(())
    }

    fn persist_root_hash(&self, root_hash: &FsNodeHash) -> Result<(), Box<dyn std::error::Error>> {
        let key = b"__ROOT_HASH__";
        let mut value = Vec::new();
        ciborium::into_writer(root_hash, &mut value)?;
        self.fs_nodes_partition.insert(key, &value)?;
        debug!("Persisted root hash {:?}", root_hash);
        Ok(())
    }

    fn persist_directory(
        &self,
        content_hash: &ContentHash,
        directory: &Directory,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = content_hash.0;
        let mut value = Vec::new();
        ciborium::into_writer(directory, &mut value)?;
        self.directories_partition.insert(&key, &value)?;
        debug!("Persisted Directory with content hash {:?}", content_hash);
        Ok(())
    }

    fn new_directory_node(
        &mut self,
        directory: Directory,
        parent_inode_number: Option<InodeNumber>,
    ) -> FsNode {
        let directory_node =
            FsNode::new_directory_node(directory.calculate_hash(), 0, parent_inode_number);

        // Persist the directory and FsNode
        if let Err(e) = self.persist_directory(&directory_node.content_hash, &directory) {
            error!("Failed to persist directory: {}", e);
        }
        if let Err(e) = self.persist_fs_node(&directory_node.calculate_hash(), &directory_node) {
            error!("Failed to persist FsNode: {}", e);
        }

        self.directories
            .insert(directory_node.content_hash, directory);
        directory_node
    }

    fn assign_inode_number(&mut self, fs_node: FsNode) -> InodeNumber {
        self.inodes.push(fs_node);
        InodeNumber((self.inodes.len() - 1) as u64)
    }

    fn new_file_node_with_persistence(
        &self,
        content_hash: ContentHash,
        size: u64,
        parent_inode_number: Option<InodeNumber>,
    ) -> FsNode {
        let fs_node = FsNode::new_file_node(content_hash, size, parent_inode_number);

        if let Err(e) = self.persist_fs_node(&fs_node.calculate_hash(), &fs_node) {
            error!("Failed to persist FsNode: {}", e);
        }

        fs_node
    }

    fn add_directory_entry(&mut self, inode_number: InodeNumber, new_entry: DirectoryEntry) {
        let directory_node = self.inodes.get(inode_number.0 as usize).unwrap().clone();
        let mut directory = self
            .directories
            .get(&directory_node.content_hash)
            .unwrap()
            .clone();
        directory.entries.push(new_entry);
        let parent_inode_number = directory.parent_inode_number;
        let new_directory_node =
            self.new_directory_node(directory, directory_node.parent_inode_number);
        self.inodes[inode_number.0 as usize] = new_directory_node;
        if let Some(parent_inode_number) = parent_inode_number {
            self.update_directory_entry(
                parent_inode_number,
                &directory_node.calculate_hash(),
                new_directory_node.calculate_hash(),
            );
        } else {
            // This is the root directory - persist the root hash
            if let Err(e) = self.persist_root_hash(&new_directory_node.calculate_hash()) {
                error!("Failed to persist root hash: {}", e);
            }
        }
    }

    fn remove_directory_entry(
        &mut self,
        inode_number: InodeNumber,
        entry_name: &str,
    ) -> Result<(), i32> {
        let (directory_node, mut directory) = self.get_directory(inode_number.0)?;

        // Find and remove the entry
        let entry_index = directory
            .entries
            .iter()
            .position(|entry| entry.name == entry_name);
        match entry_index {
            Some(index) => {
                directory.entries.remove(index);
            }
            None => return Err(libc::ENOENT),
        }

        let parent_inode_number = directory.parent_inode_number;
        let new_directory_node =
            self.new_directory_node(directory, directory_node.parent_inode_number);
        self.inodes[inode_number.0 as usize] = new_directory_node;
        if let Some(parent_inode_number) = parent_inode_number {
            self.update_directory_entry(
                parent_inode_number,
                &directory_node.calculate_hash(),
                new_directory_node.calculate_hash(),
            );
        } else {
            // This is the root directory - persist the root hash
            if let Err(e) = self.persist_root_hash(&new_directory_node.calculate_hash()) {
                error!("Failed to persist root hash: {}", e);
            }
        }
        Ok(())
    }

    fn update_directory_entry(
        &mut self,
        inode_number: InodeNumber,
        old_entry_hash: &FsNodeHash,
        new_entry_fs_node_hash: FsNodeHash,
    ) {
        let directory_node = self.inodes.get(inode_number.0 as usize).unwrap().clone();
        let mut directory = self
            .directories
            .get(&directory_node.content_hash)
            .unwrap()
            .clone();
        for entry in directory.entries.iter_mut() {
            if &entry.fs_node_hash == old_entry_hash {
                entry.fs_node_hash = new_entry_fs_node_hash;
            }
        }
        let new_directory_node =
            self.new_directory_node(directory, directory_node.parent_inode_number);
        self.inodes[inode_number.0 as usize] = new_directory_node;
        if let Some(parent_inode) = directory_node.parent_inode_number {
            self.update_directory_entry(
                parent_inode,
                &directory_node.calculate_hash(),
                new_directory_node.calculate_hash(),
            );
        } else {
            // This is the root directory - persist the root hash
            if let Err(e) = self.persist_root_hash(&new_directory_node.calculate_hash()) {
                error!("Failed to persist root hash: {}", e);
            }
        };
    }

    fn get_directory(&self, inode_number: u64) -> Result<(FsNode, Directory), i32> {
        let Some(fs_node) = self.inodes.get(inode_number as usize) else {
            return Err(libc::ENOENT);
        };
        if let FileType::Directory = fs_node.kind {
        } else {
            return Err(libc::ENOTDIR);
        };
        let Some(directory) = self.directories.get(&fs_node.content_hash) else {
            return Err(libc::ENOTDIR);
        };
        Ok((*fs_node, directory.clone()))
    }
}

impl Drop for Pfs {
    fn drop(&mut self) {
        self.keyspace
            .persist(fjall::PersistMode::SyncAll)
            .expect("To be able to persist metadata")
    }
}

impl Filesystem for Pfs {
    fn getattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: Option<u64>,
        reply: fuser::ReplyAttr,
    ) {
        let ttl = Duration::from_millis(1);

        let Some(fs_node) = self.inodes.get(ino as usize) else {
            error!("No inode found for ino: {}", ino);
            reply.error(libc::ENOENT);
            return;
        };

        let attrs = fs_node.as_file_attr(InodeNumber(ino));
        reply.attr(&ttl, &attrs);
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        let (_fs_node, directory) = match self.get_directory(ino) {
            Ok(v) => v,
            Err(e) => return reply.error(e),
        };
        if offset == 0 {
            let _ = reply.add(1, 0, fuser::FileType::Directory, ".");
            let _ = reply.add(1, 1, fuser::FileType::Directory, "..");
            let mut offset = 2;
            for entry in directory.entries.iter() {
                let _ = reply.add(
                    entry.inode_number.0 as u64,
                    offset,
                    fuser::FileType::RegularFile,
                    &entry.name,
                );
                offset += 1;
            }
        }
        reply.ok()
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
        match self.get_directory(parent) {
            Ok(_) => {}
            Err(e) => return reply.error(e),
        };
        let fs_node = self.new_directory_node(Directory::default(), Some(InodeNumber(parent)));
        let inode_number = self.assign_inode_number(fs_node);
        self.add_directory_entry(
            InodeNumber(parent),
            DirectoryEntry {
                name: name.to_str().unwrap().to_owned(),
                fs_node_hash: fs_node.calculate_hash(),
                inode_number,
            },
        );
        reply.entry(
            &Duration::from_millis(1),
            &fs_node.as_file_attr(inode_number),
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
        let temp_file_path = self.data_dir.clone() + "/" + &Utc::now().to_rfc3339();
        let fs_node = self.new_file_node_with_persistence(
            ContentHash::default(),
            0,
            Some(InodeNumber(parent)),
        );

        let inode_number = self.assign_inode_number(fs_node);
        let open_file = OpenFile {
            backing_file: File::create_new(&temp_file_path).unwrap(),
            parent_inode_number: InodeNumber(parent),
            path: PathBuf::from(temp_file_path),
            writable: (flags & O_WRONLY > 0) || (flags & O_RDWR > 0),
        };
        self.open_files.push(Some(open_file));
        self.add_directory_entry(
            InodeNumber(parent),
            DirectoryEntry {
                name: name.to_str().unwrap().to_owned(),
                fs_node_hash: fs_node.calculate_hash(),
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
        let fs_node = self.inodes.get(_ino as usize).unwrap();
        let content_hash = fs_node.content_hash;
        let path = PathBuf::from(
            self.data_dir.clone()
                + "/"
                + &base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(content_hash.0),
        );
        let open_file = OpenFile {
            backing_file: File::open(&path).unwrap(),
            parent_inode_number: fs_node.parent_inode_number.unwrap(),
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
        let _ = open_file.backing_file.write_at(data, offset as u64);
        reply.written(data.len() as u32);
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
        let Some(Some(open_file)) = self.open_files.get_mut(fh as usize) else {
            return reply.error(libc::ENOENT);
        };
        let _ = open_file.backing_file.seek(SeekFrom::Start(offset as u64));
        let mut buf = vec![0u8; size as usize];
        let _ = open_file.backing_file.read(&mut buf);
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
        let _ = fd;
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
        let old_fs_node = self.inodes.get(_ino as usize).unwrap().clone();
        let new_fs_node =
            self.new_file_node_with_persistence(content_hash, size, Some(InodeNumber(_ino)));

        self.update_directory_entry(
            open_file.parent_inode_number,
            &old_fs_node.calculate_hash(),
            new_fs_node.calculate_hash(),
        );
        self.inodes[_ino as usize] = new_fs_node;
        reply.ok()
    }

    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let (_fs_node, directory) = match self.get_directory(parent) {
            Ok(v) => v,
            Err(e) => return reply.error(e),
        };
        for entry in directory.entries.iter() {
            if &entry.name == name.to_str().unwrap() {
                let fs_node = self.inodes.get(entry.inode_number.0 as usize).unwrap();
                return reply.entry(
                    &Duration::from_millis(1),
                    &fs_node.as_file_attr(entry.inode_number),
                    0,
                );
            }
        }
        return reply.error(libc::ENOENT);
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

        // Check that the entry exists and is a file (not a directory)
        let (_fs_node, directory) = match self.get_directory(parent) {
            Ok(v) => v,
            Err(e) => return reply.error(e),
        };

        let file_entry = directory
            .entries
            .iter()
            .find(|entry| entry.name == file_name);
        let file_entry = match file_entry {
            Some(entry) => entry,
            None => return reply.error(libc::ENOENT),
        };

        // Get the file node to check if it's actually a file
        let file_node = match self.inodes.get(file_entry.inode_number.0 as usize) {
            Some(node) => node,
            None => return reply.error(libc::ENOENT),
        };

        // Only allow unlinking files, not directories
        if file_node.kind != FileType::RegularFile {
            return reply.error(libc::EISDIR);
        }

        // Remove the entry from the directory
        match self.remove_directory_entry(InodeNumber(parent), file_name) {
            Ok(()) => reply.ok(),
            Err(error) => reply.error(error),
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
        debug!(
            "Called rename with parent: {} name: {:?} newparent: {} newname: {:?} flags: {}",
            parent, name, newparent, newname, flags
        );

        let old_name = match name.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EINVAL),
        };

        let new_name = match newname.to_str() {
            Some(name) => name,
            None => return reply.error(libc::EINVAL),
        };

        // Get the source directory and find the entry to rename
        let (_source_fs_node, source_directory) = match self.get_directory(parent) {
            Ok(v) => v,
            Err(e) => return reply.error(e),
        };

        let source_entry = source_directory
            .entries
            .iter()
            .find(|entry| entry.name == old_name);
        let source_entry = match source_entry {
            Some(entry) => entry.clone(),
            None => return reply.error(libc::ENOENT),
        };

        // Verify the destination directory exists
        let (_dest_fs_node, dest_directory) = match self.get_directory(newparent) {
            Ok(v) => v,
            Err(e) => return reply.error(e),
        };

        // Check if destination already exists
        if parent == newparent && old_name == new_name {
            // Renaming to the same name in the same directory - do nothing
            return reply.ok();
        }

        // Check if destination already exists
        if dest_directory
            .entries
            .iter()
            .any(|entry| entry.name == new_name)
        {
            return reply.error(libc::EEXIST);
        }

        // Update the parent inode number in the fs_node if moving to a different directory
        if parent != newparent {
            let fs_node = self
                .inodes
                .get_mut(source_entry.inode_number.0 as usize)
                .unwrap();
            fs_node.parent_inode_number = Some(InodeNumber(newparent));
        }

        // Add the entry to the destination directory with the new name (first)
        let new_entry = DirectoryEntry {
            name: new_name.to_owned(),
            fs_node_hash: source_entry.fs_node_hash,
            inode_number: source_entry.inode_number,
        };
        self.add_directory_entry(InodeNumber(newparent), new_entry);

        // Remove the entry from the source directory (second)
        if let Err(error) = self.remove_directory_entry(InodeNumber(parent), old_name) {
            return reply.error(error);
        }

        reply.ok()
    }
}
