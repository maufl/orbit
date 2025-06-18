use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};

use parking_lot::RwLock;

use base64::Engine;
use chrono::{DateTime, Utc};
use fjall::{Config, Keyspace, PartitionHandle};
use fuser::{FileAttr, Filesystem};
use libc::{O_RDWR, O_WRONLY};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

pub mod network;

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Clone, Copy)]
pub struct ContentHash([u8; 32]);

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Clone, Copy)]
pub struct FsNodeHash(pub [u8; 32]);

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Clone, Copy)]
pub struct InodeNumber(pub u64);

fn calculate_hash<T: Serialize>(v: &T) -> [u8; 32] {
    let mut buffer = Vec::new();
    ciborium::into_writer(v, &mut buffer).expect("To be able to serialize");
    hmac_sha256::Hash::hash(&buffer)
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum FileType {
    #[default]
    Directory,
    RegularFile,
    Symlink,
}

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
pub struct FsNode {
    /// (recursive?) size in bytes
    pub size: u64,
    /// Time of last change to the content
    pub modification_time: DateTime<Utc>,
    /// The hash of the content, SHA256 probably
    pub content_hash: ContentHash,
    /// Kind of file
    pub kind: FileType,
    #[serde(skip_serializing, skip_deserializing)]
    pub parent_inode_number: Option<InodeNumber>,
}

impl FsNode {
    fn is_directory(&self) -> bool {
        matches!(self.kind, FileType::Directory)
    }

    fn as_file_attr(&self, inode_number: InodeNumber) -> FileAttr {
        let is_directory = self.is_directory();
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

    pub fn calculate_hash(&self) -> FsNodeHash {
        FsNodeHash(calculate_hash(&self))
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct DirectoryEntry {
    name: String,
    fs_node_hash: FsNodeHash,
    #[serde(skip_serializing, skip_deserializing)]
    inode_number: InodeNumber,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Directory {
    pub entries: Vec<DirectoryEntry>,
    #[serde(skip_serializing, skip_deserializing)]
    pub parent_inode_number: Option<InodeNumber>,
}

impl Directory {
    pub fn calculate_hash(&self) -> ContentHash {
        ContentHash(calculate_hash(&self))
    }

    fn entry_by_name(&self, name: &str) -> Option<&DirectoryEntry> {
        self.entries.iter().find(|e| e.name == name)
    }
}

struct OpenFile {
    path: PathBuf,
    backing_file: File,
    parent_inode_number: InodeNumber,
    writable: bool,
}

struct PfsRuntimeData {
    directories: BTreeMap<ContentHash, Directory>,
    inodes: Vec<FsNode>,
    open_files: Vec<Option<OpenFile>>,
}

#[derive(Clone)]
pub struct Pfs {
    runtime_data: Arc<RwLock<PfsRuntimeData>>,
    data_dir: String,
    /// This field may not be removed! It looks unused, but if you drop the Keyspace fjall will stop working
    keyspace: Keyspace,
    pub fs_nodes_partition: PartitionHandle,
    pub directories_partition: PartitionHandle,
    root_node_hash_sender: Option<Sender<(FsNodeHash, FsNodeHash)>>,
}

impl Pfs {
    pub fn initialize(
        data_dir: String,
        root_node_hash_sender: Option<Sender<(FsNodeHash, FsNodeHash)>>,
    ) -> Result<Pfs, Box<dyn std::error::Error>> {
        let kv_path = format!("{}/metadata", data_dir);
        std::fs::create_dir_all(&kv_path)?;

        let keyspace = Config::new(&kv_path).open()?;
        let fs_nodes_partition = keyspace.open_partition("fs_nodes", Default::default())?;
        let directories_partition = keyspace.open_partition("directories", Default::default())?;

        let mut pfs = Pfs {
            data_dir: data_dir.clone(),
            runtime_data: Arc::new(RwLock::new(PfsRuntimeData {
                directories: BTreeMap::new(),
                inodes: vec![FsNode::default(); 1],
                open_files: Vec::new(),
            })),
            keyspace,
            fs_nodes_partition,
            directories_partition,
            root_node_hash_sender,
        };

        // Try to load existing data
        if let Err(e) = pfs.restore_filesystem_tree() {
            warn!("Failed to load persisted data, starting fresh: {}", e);
            // Start fresh if loading fails
            {
                let mut runtime_data = pfs.runtime_data.write();
                runtime_data.directories.clear();
                runtime_data.inodes = vec![FsNode::default(); 1];
            }
            let fs_node = pfs.new_directory_node(Directory::default(), None);
            pfs.assign_inode_number(fs_node.clone());
            if let Err(e) = pfs.persist_root_hash(&fs_node.calculate_hash()) {
                error!("Failed to persist root hash: {}", e);
            }
        }

        Ok(pfs)
    }

    pub fn get_root_node(&self) -> FsNode {
        self.runtime_data.read().inodes[1]
    }

    pub fn diff(
        &self,
        old_root_node: &FsNode,
        new_root_node: &FsNode,
    ) -> (Vec<FsNode>, Vec<Directory>) {
        let mut fs_nodes = Vec::new();
        let mut directories = Vec::new();
        self.diff_recursive(
            old_root_node,
            new_root_node,
            &mut fs_nodes,
            &mut directories,
        );
        (fs_nodes, directories)
    }

    fn diff_recursive(
        &self,
        old_dir_node: &FsNode,
        new_dir_node: &FsNode,
        fs_nodes: &mut Vec<FsNode>,
        directories: &mut Vec<Directory>,
    ) {
        let runtime_data = self.runtime_data.read();
        let current_dirs = &runtime_data.directories;
        let old_directory = current_dirs
            .get(&old_dir_node.content_hash)
            .unwrap()
            .clone();
        let new_directory = current_dirs
            .get(&new_dir_node.content_hash)
            .unwrap()
            .clone();
        for entry in new_directory.entries.iter() {
            if !old_directory.entries.iter().any(|e| e.name == entry.name) {
                // New entry!
                let new_fs_node = runtime_data.inodes[entry.inode_number.0 as usize];
                if new_fs_node.is_directory() {
                    // Simplification, we assume that directories will always be created empty and that we don't need to recurse
                    let new_directory = runtime_data
                        .directories
                        .get(&new_fs_node.content_hash)
                        .unwrap()
                        .clone();
                    directories.push(new_directory);
                }
                fs_nodes.push(new_fs_node);
            }
            if old_directory
                .entries
                .iter()
                .any(|e| e.name == entry.name && e.fs_node_hash != entry.fs_node_hash)
            {
                // Changed entry!
                let changed_fs_node = runtime_data.inodes[entry.inode_number.0 as usize];
                if changed_fs_node.is_directory() {
                    let old_entry = old_directory
                        .entries
                        .iter()
                        .find(|e| e.name == entry.name)
                        .unwrap();
                    let old_fs_node = self
                        .load_fs_node(&old_entry.fs_node_hash)
                        .expect("To find old FsNode");
                    self.diff_recursive(&old_fs_node, &changed_fs_node, fs_nodes, directories);
                } else {
                    fs_nodes.push(changed_fs_node);
                };
            }
        }
        fs_nodes.push(new_dir_node.clone());
        directories.push(new_directory);
    }

    pub fn load_fs_node(&self, node_hash: &FsNodeHash) -> Result<FsNode, anyhow::Error> {
        if let Some(value) = self.fs_nodes_partition.get(&node_hash.0)? {
            let fs_node: FsNode = ciborium::from_reader(&*value)?;
            return Ok(fs_node);
        }
        Err(anyhow::anyhow!(
            "FsNode with hash {:?} not found",
            node_hash
        ))
    }

    fn load_directory(&self, content_hash: &ContentHash) -> Result<Directory, anyhow::Error> {
        if let Some(value) = self.directories_partition.get(&content_hash.0)? {
            let directory: Directory = ciborium::from_reader(&*value)?;
            return Ok(directory);
        }
        Err(anyhow::anyhow!(
            "Directory with hash {:?} not found",
            content_hash
        ))
    }

    pub fn update_directory_recursive(
        &mut self,
        old_dir_hash: &FsNodeHash,
        new_dir_hash: &FsNodeHash,
        inode_number: InodeNumber,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Load the old and new directory FsNodes
        let old_dir_node = self.load_fs_node(old_dir_hash)?;
        let new_dir_node = self.load_fs_node(new_dir_hash)?;

        // Both should be directories
        if !old_dir_node.is_directory() || !new_dir_node.is_directory() {
            return Err("Both nodes must be directories".into());
        }

        // Load the directory contents
        let old_directory = self.load_directory(&old_dir_node.content_hash)?;
        let mut new_directory = self.load_directory(&new_dir_node.content_hash)?;
        new_directory.parent_inode_number = old_directory.parent_inode_number;

        // Compare entries and handle changes
        for new_entry in &mut new_directory.entries {
            // Check if this is a new entry
            let old_entry = old_directory.entry_by_name(&new_entry.name);
            if let Some(old_entry) = old_entry {
                let new_fs_node = self.load_fs_node(&new_entry.fs_node_hash)?;
                let old_fs_node = self.load_fs_node(&old_entry.fs_node_hash)?;

                if old_fs_node.is_directory() && new_fs_node.is_directory() {
                    let _ = self.update_directory_recursive(
                        &old_fs_node.calculate_hash(),
                        &new_fs_node.calculate_hash(),
                        old_entry.inode_number,
                    );
                    new_entry.inode_number = old_entry.inode_number;
                } else if !old_fs_node.is_directory() && new_fs_node.is_directory() {
                    // Assign a new inode number for this entry
                    new_entry.inode_number =
                        self.restore_node_recursive(&new_entry.fs_node_hash, Some(inode_number))?;
                } else {
                    new_entry.inode_number = old_entry.inode_number;
                    self.runtime_data.write().inodes[new_entry.inode_number.0 as usize] =
                        new_fs_node;
                }
            } else {
                // new entry
                new_entry.inode_number =
                    self.restore_node_recursive(&new_entry.fs_node_hash, Some(inode_number))?;
            }
        }

        // Update the directory in our runtime data with corrected inode numbers
        self.runtime_data
            .write()
            .directories
            .insert(new_dir_node.content_hash, new_directory);

        Ok(())
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
            self.runtime_data.read().inodes.len()
        );
        Ok(())
    }

    fn restore_node_recursive(
        &mut self,
        fs_node_hash: &FsNodeHash,
        parent_inode_number: Option<InodeNumber>,
    ) -> Result<InodeNumber, Box<dyn std::error::Error>> {
        let mut fs_node = self.load_fs_node(fs_node_hash)?;
        fs_node.parent_inode_number = parent_inode_number;
        let inode_number = self.assign_inode_number(fs_node);
        debug!(
            "Restored fs_node with hash {:?} to inode {:?}, type: {:?}",
            fs_node_hash, inode_number, fs_node.kind
        );
        if fs_node.is_directory() {
            // Load the directory structure
            match self.load_directory(&fs_node.content_hash) {
                Ok(mut directory) => {
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
                    self.runtime_data
                        .write()
                        .directories
                        .insert(fs_node.content_hash, directory);
                }
                Err(e) => {
                    warn!(
                        "Directory content not found for FsNode with hash {:?}: {}",
                        fs_node.content_hash, e
                    );
                }
            }
        }
        Ok(inode_number)
    }

    pub fn persist_fs_node(&self, fs_node: &FsNode) -> Result<(), Box<dyn std::error::Error>> {
        let node_hash = fs_node.calculate_hash();
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

    pub fn persist_directory(
        &self,
        directory: &Directory,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let content_hash = directory.calculate_hash();
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
        if let Err(e) = self.persist_directory(&directory) {
            error!("Failed to persist directory: {}", e);
        }
        if let Err(e) = self.persist_fs_node(&directory_node) {
            error!("Failed to persist FsNode: {}", e);
        }

        self.runtime_data
            .write()
            .directories
            .insert(directory_node.content_hash, directory);
        directory_node
    }

    fn assign_inode_number(&mut self, fs_node: FsNode) -> InodeNumber {
        let mut runtime_data = self.runtime_data.write();
        runtime_data.inodes.push(fs_node);
        InodeNumber((runtime_data.inodes.len() - 1) as u64)
    }

    fn new_file_node_with_persistence(
        &self,
        content_hash: ContentHash,
        size: u64,
        parent_inode_number: Option<InodeNumber>,
    ) -> FsNode {
        let fs_node = FsNode::new_file_node(content_hash, size, parent_inode_number);

        if let Err(e) = self.persist_fs_node(&fs_node) {
            error!("Failed to persist FsNode: {}", e);
        }

        fs_node
    }

    fn add_directory_entry(
        &mut self,
        inode_number: InodeNumber,
        new_entry: DirectoryEntry,
    ) -> Result<(), i32> {
        let (directory_node, mut directory) = self.get_directory(inode_number.0)?;
        directory.entries.push(new_entry);
        self.update_directory(inode_number, &directory_node, directory)?;
        Ok(())
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
        self.update_directory(inode_number, &directory_node, directory)?;
        Ok(())
    }

    fn update_directory_entry(
        &mut self,
        inode_number: InodeNumber,
        old_entry_hash: &FsNodeHash,
        new_entry_fs_node_hash: FsNodeHash,
    ) -> Result<(), i32> {
        let (directory_node, mut directory) = self.get_directory(inode_number.0)?;
        for entry in directory.entries.iter_mut() {
            if &entry.fs_node_hash == old_entry_hash {
                entry.fs_node_hash = new_entry_fs_node_hash;
            }
        }
        self.update_directory(inode_number, &directory_node, directory)?;
        Ok(())
    }

    fn update_directory(
        &mut self,
        inode_number: InodeNumber,
        old_directory_node: &FsNode,
        new_directory: Directory,
    ) -> Result<(), i32> {
        let new_directory_node =
            self.new_directory_node(new_directory, old_directory_node.parent_inode_number);
        self.runtime_data.write().inodes[inode_number.0 as usize] = new_directory_node;
        if let Some(parent_inode) = old_directory_node.parent_inode_number {
            self.update_directory_entry(
                parent_inode,
                &old_directory_node.calculate_hash(),
                new_directory_node.calculate_hash(),
            )?;
        } else {
            let old_root_hash = old_directory_node.calculate_hash();
            let root_hash = new_directory_node.calculate_hash();
            // This is the root directory - persist the root hash
            if let Err(e) = self.persist_root_hash(&new_directory_node.calculate_hash()) {
                error!("Failed to persist root hash: {}", e);
            }
            if let Some(ref mut sender) = self.root_node_hash_sender {
                let _ = sender.blocking_send((old_root_hash, root_hash));
            }
        };
        Ok(())
    }

    fn get_directory(&self, inode_number: u64) -> Result<(FsNode, Directory), i32> {
        let runtime_data = self.runtime_data.read();
        let Some(fs_node) = runtime_data.inodes.get(inode_number as usize) else {
            return Err(libc::ENOENT);
        };
        if !fs_node.is_directory() {
            return Err(libc::ENOTDIR);
        }
        let Some(directory) = runtime_data.directories.get(&fs_node.content_hash) else {
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

        let runtime_data = self.runtime_data.read();
        let Some(fs_node) = runtime_data.inodes.get(ino as usize) else {
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
        let (fs_node, directory) = match self.get_directory(ino) {
            Ok(v) => v,
            Err(e) => return reply.error(e),
        };
        if offset == 0 {
            let _ = reply.add(1, ino as i64, fuser::FileType::Directory, ".");
            let _ = reply.add(
                1,
                fs_node.parent_inode_number.map(|i| i.0).unwrap_or(ino) as i64,
                fuser::FileType::Directory,
                "..",
            );
            let mut offset = 2;
            let runtime_data = self.runtime_data.read();
            let inodes = &runtime_data.inodes;
            for entry in directory.entries.iter() {
                let fs_node = inodes[entry.inode_number.0 as usize];
                let _ = reply.add(
                    entry.inode_number.0 as u64,
                    offset,
                    if fs_node.is_directory() {
                        fuser::FileType::Directory
                    } else {
                        fuser::FileType::RegularFile
                    },
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
        if let Err(error) = self.add_directory_entry(
            InodeNumber(parent),
            DirectoryEntry {
                name: name.to_str().unwrap().to_owned(),
                fs_node_hash: fs_node.calculate_hash(),
                inode_number,
            },
        ) {
            return reply.error(error);
        };
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
        self.runtime_data.write().open_files.push(Some(open_file));
        if let Err(error) = self.add_directory_entry(
            InodeNumber(parent),
            DirectoryEntry {
                name: name.to_str().unwrap().to_owned(),
                fs_node_hash: fs_node.calculate_hash(),
                inode_number,
            },
        ) {
            return reply.error(error);
        };
        reply.created(
            &Duration::from_secs(60),
            &fs_node.as_file_attr(inode_number),
            0,
            (self.runtime_data.read().open_files.len() - 1)
                .try_into()
                .unwrap(),
            0,
        );
    }

    fn open(&mut self, _req: &fuser::Request<'_>, _ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        let writable = _flags & libc::O_WRONLY > 0 || _flags & libc::O_RDWR > 0;
        if writable {
            warn!("Tried to open a file in write mode");
            return reply.error(libc::ENOSYS);
        };
        let fs_node = self.runtime_data.read().inodes[_ino as usize];
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
        let mut runtime_data = self.runtime_data.write();
        runtime_data.open_files.push(Some(open_file));
        let fh = (runtime_data.open_files.len() - 1).try_into().unwrap();
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
        let mut runtime_data = self.runtime_data.write();
        let open_file = runtime_data.open_files[fh as usize].as_mut().unwrap();
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
        let mut runtime_data = self.runtime_data.write();
        let Some(Some(open_file)) = runtime_data.open_files.get_mut(fh as usize) else {
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
        let mut open_file = {
            let mut runtime_data = self.runtime_data.write();
            std::mem::replace(&mut runtime_data.open_files[fh as usize], None).unwrap()
        };
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
        let old_fs_node = self.runtime_data.read().inodes[_ino as usize];
        let new_fs_node =
            self.new_file_node_with_persistence(content_hash, size, Some(InodeNumber(_ino)));

        if let Err(error) = self.update_directory_entry(
            open_file.parent_inode_number,
            &old_fs_node.calculate_hash(),
            new_fs_node.calculate_hash(),
        ) {
            return reply.error(error);
        };
        self.runtime_data.write().inodes[_ino as usize] = new_fs_node;
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
        let runtime_data = self.runtime_data.read();
        for entry in directory.entries.iter() {
            if &entry.name == name.to_str().unwrap() {
                let fs_node = &runtime_data.inodes[entry.inode_number.0 as usize];
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
        let file_node_kind = {
            let runtime_data = self.runtime_data.read();
            match runtime_data.inodes.get(file_entry.inode_number.0 as usize) {
                Some(node) => node.kind,
                None => return reply.error(libc::ENOENT),
            }
        };

        // Only allow unlinking files, not directories
        if file_node_kind != FileType::RegularFile {
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
            let mut runtime_data = self.runtime_data.write();
            let fs_node = runtime_data
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
        if let Err(error) = self.add_directory_entry(InodeNumber(newparent), new_entry) {
            return reply.error(error);
        }

        // Remove the entry from the source directory (second)
        if let Err(error) = self.remove_directory_entry(InodeNumber(parent), old_name) {
            return reply.error(error);
        }

        reply.ok()
    }
}
