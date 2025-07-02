use std::{
    collections::BTreeMap,
    fmt::Display,
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};

use network::{Messages, NetworkCommunication};
use parking_lot::RwLock;

use base64::Engine;
use chrono::{DateTime, Utc};
use fuser::{FileAttr, Filesystem};
use libc::{O_RDWR, O_WRONLY};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};

use crate::persistence::{Persistence, PfsPersistence};

pub mod config;
pub mod network;
pub mod persistence;

#[derive(Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Clone, Copy)]
pub struct ContentHash(pub [u8; 32]);

impl Display for ContentHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ct-{}", hex::encode(self.0))
    }
}

impl std::fmt::Debug for ContentHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

#[derive(Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Clone, Copy)]
pub struct FsNodeHash(pub [u8; 32]);

impl Display for FsNodeHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fsn-{}", hex::encode(self.0))
    }
}

impl std::fmt::Debug for FsNodeHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

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
pub struct RuntimeFsNode {
    /// (recursive?) size in bytes
    pub size: u64,
    /// Time of last change to the content
    pub modification_time: DateTime<Utc>,
    /// The hash of the content, SHA256 probably
    pub content_hash: ContentHash,
    /// Kind of file
    pub kind: FileType,
    #[serde(skip)]
    pub parent_inode_number: Option<InodeNumber>,
}

impl RuntimeFsNode {
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
    ) -> RuntimeFsNode {
        RuntimeFsNode {
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
    ) -> RuntimeFsNode {
        RuntimeFsNode {
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
}

impl FsNode {
    fn is_directory(&self) -> bool {
        matches!(self.kind, FileType::Directory)
    }

    pub fn calculate_hash(&self) -> FsNodeHash {
        FsNodeHash(calculate_hash(&self))
    }

    pub fn as_runtime_fs_node(&self, parent_inode_number: Option<InodeNumber>) -> RuntimeFsNode {
        RuntimeFsNode {
            size: self.size,
            modification_time: self.modification_time,
            content_hash: self.content_hash,
            kind: self.kind,
            parent_inode_number,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct RuntimeDirectoryEntry {
    pub name: String,
    pub fs_node_hash: FsNodeHash,
    #[serde(skip)]
    pub inode_number: InodeNumber,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct DirectoryEntry {
    pub name: String,
    pub fs_node_hash: FsNodeHash,
}

impl DirectoryEntry {
    pub fn as_runtime_directory_entry(&self, inode_number: InodeNumber) -> RuntimeDirectoryEntry {
        RuntimeDirectoryEntry {
            name: self.name.clone(),
            fs_node_hash: self.fs_node_hash,
            inode_number,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct RuntimeDirectory {
    pub entries: Vec<RuntimeDirectoryEntry>,
}

impl RuntimeDirectory {
    pub fn calculate_hash(&self) -> ContentHash {
        ContentHash(calculate_hash(&self))
    }

    fn entry_by_name(&self, name: &str) -> Option<&RuntimeDirectoryEntry> {
        self.entries.iter().find(|e| e.name == name)
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct Directory {
    pub entries: Vec<DirectoryEntry>,
}

impl Directory {
    pub fn calculate_hash(&self) -> ContentHash {
        ContentHash(calculate_hash(&self))
    }
}

impl From<&RuntimeFsNode> for FsNode {
    fn from(runtime_node: &RuntimeFsNode) -> Self {
        FsNode {
            size: runtime_node.size,
            modification_time: runtime_node.modification_time,
            content_hash: runtime_node.content_hash,
            kind: runtime_node.kind,
        }
    }
}

impl From<RuntimeFsNode> for FsNode {
    fn from(runtime_node: RuntimeFsNode) -> Self {
        FsNode {
            size: runtime_node.size,
            modification_time: runtime_node.modification_time,
            content_hash: runtime_node.content_hash,
            kind: runtime_node.kind,
        }
    }
}

impl From<&RuntimeDirectoryEntry> for DirectoryEntry {
    fn from(runtime_entry: &RuntimeDirectoryEntry) -> Self {
        DirectoryEntry {
            name: runtime_entry.name.clone(),
            fs_node_hash: FsNodeHash(runtime_entry.fs_node_hash.0),
        }
    }
}

impl From<&RuntimeDirectory> for Directory {
    fn from(runtime_directory: &RuntimeDirectory) -> Self {
        Directory {
            entries: runtime_directory.entries.iter().map(|e| e.into()).collect(),
        }
    }
}

struct OpenFile {
    path: PathBuf,
    backing_file: File,
    parent_inode_number: InodeNumber,
    writable: bool,
}

#[derive(Debug, Clone)]
pub struct RuntimeDirectoryEntryInfo {
    pub ino: u64,
    pub offset: i64,
    pub file_type: fuser::FileType,
    pub name: String,
}

struct PfsRuntimeData {
    directories: BTreeMap<ContentHash, RuntimeDirectory>,
    inodes: Vec<RuntimeFsNode>,
    open_files: Vec<Option<OpenFile>>,
}

#[derive(Clone)]
pub struct Pfs {
    runtime_data: Arc<RwLock<PfsRuntimeData>>,
    pub data_dir: String,
    pub persistence: Arc<dyn Persistence>,
    network_communication: Option<Arc<dyn NetworkCommunication>>,
}

impl Pfs {
    pub fn initialize(
        data_dir: String,
        network_communication: Option<Arc<dyn NetworkCommunication>>,
    ) -> Result<Pfs, Box<dyn std::error::Error>> {
        let persistence = Arc::new(PfsPersistence::new(&data_dir)?);

        let mut pfs = Pfs {
            data_dir: data_dir.clone(),
            runtime_data: Arc::new(RwLock::new(PfsRuntimeData {
                directories: BTreeMap::new(),
                inodes: vec![RuntimeFsNode::default(); 1],
                open_files: Vec::new(),
            })),
            persistence,
            network_communication,
        };

        // Try to load existing data
        if let Err(e) = pfs.restore_filesystem_tree() {
            warn!("Failed to load persisted data, starting fresh: {}", e);
            // Start fresh if loading fails
            {
                let mut runtime_data = pfs.runtime_data.write();
                runtime_data.directories.clear();
                runtime_data.inodes = vec![RuntimeFsNode::default(); 1];
            }
            let initial_directory = RuntimeDirectory::default();
            let initial_root_node = RuntimeFsNode {
                content_hash: initial_directory.calculate_hash(),
                modification_time: DateTime::<Utc>::default(),
                kind: FileType::Directory,
                parent_inode_number: None,
                size: 0,
            };
            pfs.persistence
                .persist_directory(&(&initial_directory).into())?;
            pfs.persistence.persist_fs_node(&initial_root_node.into())?;
            pfs.runtime_data
                .write()
                .directories
                .insert(initial_root_node.content_hash, initial_directory);
            pfs.assign_inode_number(initial_root_node.clone());
            if let Err(e) = pfs
                .persistence
                .persist_root_hash(&initial_root_node.calculate_hash())
            {
                error!("Failed to persist root hash: {}", e);
            }
        }

        Ok(pfs)
    }

    pub fn get_root_node(&self) -> RuntimeFsNode {
        self.runtime_data.read().inodes[1]
    }


    pub fn update_directory_recursive(
        &mut self,
        old_dir_hash: &FsNodeHash,
        new_dir_hash: &FsNodeHash,
        inode_number: InodeNumber,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Load the old and new directory RuntimeFsNodes
        let old_dir_node = self.persistence.load_fs_node(old_dir_hash)?;
        let new_dir_node = self.persistence.load_fs_node(new_dir_hash)?;

        // Both should be directories
        if !old_dir_node.is_directory() || !new_dir_node.is_directory() {
            return Err("Both nodes must be directories".into());
        }

        // Load the directory contents - use runtime state for old directory since it has corrected inode numbers
        let old_directory = {
            let runtime_data = self.runtime_data.read();
            runtime_data
                .directories
                .get(&old_dir_node.content_hash)
                .ok_or_else(|| "Old directory not found in runtime state")?
                .clone()
        };
        let mut new_directory = self
            .persistence
            .load_directory(&new_dir_node.content_hash)?;

        let mut new_entries = Vec::new();
        // Compare entries and handle changes
        for new_entry in &mut new_directory.entries {
            // Check if this is a new entry
            let old_entry = old_directory.entry_by_name(&new_entry.name);
            let inode_number = if let Some(old_entry) = old_entry {
                let new_fs_node = self.persistence.load_fs_node(&new_entry.fs_node_hash)?;
                let old_fs_node = self.persistence.load_fs_node(&old_entry.fs_node_hash)?;

                if old_fs_node.is_directory() && new_fs_node.is_directory() {
                    self.update_directory_recursive(
                        &old_fs_node.calculate_hash(),
                        &new_fs_node.calculate_hash(),
                        old_entry.inode_number,
                    )?;
                    old_entry.inode_number
                } else if !old_fs_node.is_directory() && new_fs_node.is_directory() {
                    // Assign a new inode number for this entry
                    self.restore_node_recursive(&new_entry.fs_node_hash, Some(inode_number))?
                } else {
                    let new_fs_node = new_fs_node.as_runtime_fs_node(Some(inode_number));
                    self.runtime_data.write().inodes[old_entry.inode_number.0 as usize] =
                        new_fs_node;
                    old_entry.inode_number
                }
            } else {
                // new entry
                self.restore_node_recursive(&new_entry.fs_node_hash, Some(inode_number))?
            };
            new_entries.push(new_entry.as_runtime_directory_entry(inode_number));
        }

        // Update the directory in our runtime data with corrected inode numbers
        self.runtime_data.write().directories.insert(
            new_dir_node.content_hash,
            RuntimeDirectory {
                entries: new_entries,
            },
        );

        // Fix the parent inode number for the directory node before storing it
        // Get the current parent from the existing node if it exists
        let new_dir_node = if let Some(existing_node) =
            self.runtime_data.read().inodes.get(inode_number.0 as usize)
        {
            new_dir_node.as_runtime_fs_node(existing_node.parent_inode_number)
        } else {
            new_dir_node.as_runtime_fs_node(None)
        };
        self.runtime_data.write().inodes[inode_number.0 as usize] = new_dir_node;
        if inode_number.0 == 1 {
            self.persistence
                .persist_root_hash(&new_dir_node.calculate_hash())?;
        }

        Ok(())
    }

    fn restore_filesystem_tree(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Load the root hash first
        let root_hash = self.persistence.load_root_hash()?;
        info!("Found root hash {} in persistent storage", root_hash);

        // Restore the filesystem tree recursively, starting from the root
        let root_inode = self.restore_node_recursive(&root_hash, None)?;
        info!("Restored root node to inode {}", root_inode.0);

        info!(
            "Successfully restored filesystem tree from persistent storage with {} inodes",
            self.runtime_data.read().inodes.len() - 1 // index 0 is not a valid FS node
        );
        Ok(())
    }

    fn restore_node_recursive(
        &mut self,
        fs_node_hash: &FsNodeHash,
        parent_inode_number: Option<InodeNumber>,
    ) -> Result<InodeNumber, Box<dyn std::error::Error>> {
        let fs_node = self
            .persistence
            .load_fs_node(fs_node_hash)?
            .as_runtime_fs_node(parent_inode_number);
        let inode_number = self.assign_inode_number(fs_node);
        if fs_node.is_directory() {
            // Load the directory structure
            match self.persistence.load_directory(&fs_node.content_hash) {
                Ok(mut directory) => {
                    // Restore each entry in the directory, updating with correct inode numbers
                    let mut new_entries = Vec::new();
                    for entry in &mut directory.entries {
                        let child_inode_number =
                            self.restore_node_recursive(&entry.fs_node_hash, Some(inode_number))?;
                        new_entries.push(entry.as_runtime_directory_entry(child_inode_number));
                    }

                    // Store the updated directory
                    self.runtime_data.write().directories.insert(
                        fs_node.content_hash,
                        RuntimeDirectory {
                            entries: new_entries,
                        },
                    );
                }
                Err(e) => {
                    warn!(
                        "RuntimeDirectory content not found for RuntimeFsNode with hash {}: {}",
                        fs_node.content_hash, e
                    );
                }
            }
        }
        Ok(inode_number)
    }

    fn new_directory_node(
        &mut self,
        directory: RuntimeDirectory,
        parent_inode_number: Option<InodeNumber>,
    ) -> RuntimeFsNode {
        let directory_node =
            RuntimeFsNode::new_directory_node(directory.calculate_hash(), 0, parent_inode_number);

        // Persist the directory and RuntimeFsNode
        if let Err(e) = self.persistence.persist_directory(&(&directory).into()) {
            error!("Failed to persist directory: {}", e);
        }
        if let Err(e) = self.persistence.persist_fs_node(&directory_node.into()) {
            error!("Failed to persist RuntimeFsNode: {}", e);
        }

        self.runtime_data
            .write()
            .directories
            .insert(directory_node.content_hash, directory);
        directory_node
    }

    fn assign_inode_number(&mut self, fs_node: RuntimeFsNode) -> InodeNumber {
        let mut runtime_data = self.runtime_data.write();
        runtime_data.inodes.push(fs_node);
        InodeNumber((runtime_data.inodes.len() - 1) as u64)
    }

    fn new_file_node_with_persistence(
        &self,
        content_hash: ContentHash,
        size: u64,
        parent_inode_number: Option<InodeNumber>,
    ) -> RuntimeFsNode {
        let fs_node = RuntimeFsNode::new_file_node(content_hash, size, parent_inode_number);

        if let Err(e) = self.persistence.persist_fs_node(&fs_node.into()) {
            error!("Failed to persist RuntimeFsNode: {}", e);
        }

        fs_node
    }

    fn add_directory_entry(
        &mut self,
        inode_number: InodeNumber,
        new_entry: RuntimeDirectoryEntry,
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
        old_directory_node: &RuntimeFsNode,
        new_directory: RuntimeDirectory,
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
            // This is the root directory - persist the root hash
            if let Err(e) = self
                .persistence
                .persist_root_hash(&new_directory_node.calculate_hash())
            {
                error!("Failed to persist root hash: {}", e);
            }
            self.sent_messages_for_changed_root(old_root_hash);
        };
        Ok(())
    }

    fn get_directory(&self, inode_number: u64) -> Result<(RuntimeFsNode, RuntimeDirectory), i32> {
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

    fn sent_messages_for_changed_root(&self, old_hash: FsNodeHash) {
        let Some(ref network_comm) = self.network_communication else {
            return;
        };
        let new_hash = self.get_root_node().calculate_hash();
        info!("Root hash changes, sending updates to peer");
        let old_node = self
            .persistence
            .load_fs_node(&old_hash)
            .expect("to find node");
        let new_node = self
            .persistence
            .load_fs_node(&new_hash)
            .expect("to find node");
        let (updated_fs_nodes, updated_directories) = self.persistence.diff(&old_node, &new_node);
        network_comm.send_message(Messages::NewFsNodes(updated_fs_nodes));
        network_comm.send_message(Messages::NewDirectories(updated_directories));
        network_comm.send_message(Messages::RootHashChanged(new_hash.0));
    }

    // Business logic methods without FUSE-specific handling
    pub fn pfs_getattr(&self, ino: u64) -> Result<FileAttr, libc::c_int> {
        let runtime_data = self.runtime_data.read();
        let Some(fs_node) = runtime_data.inodes.get(ino as usize) else {
            return Err(libc::ENOENT);
        };
        let attrs = fs_node.as_file_attr(InodeNumber(ino));
        Ok(attrs)
    }

    pub fn pfs_readdir(
        &self,
        ino: u64,
        offset: i64,
    ) -> Result<Vec<RuntimeDirectoryEntryInfo>, libc::c_int> {
        let (fs_node, directory) = self.get_directory(ino)?;
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

            let runtime_data = self.runtime_data.read();
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

    fn pfs_mkdir(&mut self, parent: u64, name: &str) -> Result<FileAttr, libc::c_int> {
        match self.get_directory(parent) {
            Ok(_) => {}
            Err(e) => return Err(e),
        };
        let fs_node =
            self.new_directory_node(RuntimeDirectory::default(), Some(InodeNumber(parent)));
        let inode_number = self.assign_inode_number(fs_node);
        self.add_directory_entry(
            InodeNumber(parent),
            RuntimeDirectoryEntry {
                name: name.to_owned(),
                fs_node_hash: fs_node.calculate_hash(),
                inode_number,
            },
        )?;
        Ok(fs_node.as_file_attr(inode_number))
    }

    fn pfs_create(
        &mut self,
        parent: u64,
        name: &str,
        flags: i32,
    ) -> Result<(FileAttr, u64), libc::c_int> {
        let temp_file_path = self.data_dir.clone() + "/" + &Utc::now().to_rfc3339();
        let fs_node = self.new_file_node_with_persistence(
            ContentHash::default(),
            0,
            Some(InodeNumber(parent)),
        );

        let inode_number = self.assign_inode_number(fs_node);
        let open_file = OpenFile {
            backing_file: File::create_new(&temp_file_path).map_err(|_| libc::EIO)?,
            parent_inode_number: InodeNumber(parent),
            path: PathBuf::from(temp_file_path),
            writable: (flags & O_WRONLY > 0) || (flags & O_RDWR > 0),
        };
        self.runtime_data.write().open_files.push(Some(open_file));
        self.add_directory_entry(
            InodeNumber(parent),
            RuntimeDirectoryEntry {
                name: name.to_owned(),
                fs_node_hash: fs_node.calculate_hash(),
                inode_number,
            },
        )?;
        let fh = (self.runtime_data.read().open_files.len() - 1) as u64;
        Ok((fs_node.as_file_attr(inode_number), fh))
    }

    fn pfs_open(&mut self, ino: u64, flags: i32) -> Result<u64, libc::c_int> {
        let writable = flags & libc::O_WRONLY > 0 || flags & libc::O_RDWR > 0;
        if writable {
            return Err(libc::ENOSYS);
        }
        let fs_node = self.runtime_data.read().inodes[ino as usize];
        let content_hash = fs_node.content_hash;
        let path = PathBuf::from(
            self.data_dir.clone()
                + "/"
                + &base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(content_hash.0),
        );
        let backing_file = File::open(&path).map_err(|_| libc::ENOENT)?;
        let open_file = OpenFile {
            backing_file,
            parent_inode_number: fs_node.parent_inode_number.unwrap(),
            path,
            writable,
        };
        let mut runtime_data = self.runtime_data.write();
        runtime_data.open_files.push(Some(open_file));
        let fh = (runtime_data.open_files.len() - 1) as u64;
        Ok(fh)
    }

    fn pfs_write(
        &mut self,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
    ) -> Result<u32, libc::c_int> {
        let mut runtime_data = self.runtime_data.write();
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

    fn pfs_read(
        &mut self,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
    ) -> Result<Vec<u8>, libc::c_int> {
        let mut runtime_data = self.runtime_data.write();
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

    fn pfs_release(&mut self, ino: u64, fh: u64) -> Result<(), libc::c_int> {
        let mut open_file = {
            let mut runtime_data = self.runtime_data.write();
            std::mem::replace(&mut runtime_data.open_files[fh as usize], None)
                .ok_or(libc::ENOENT)?
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
        let new_file_path = self.data_dir.clone()
            + "/"
            + &base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(content_hash.0);
        std::fs::rename(&open_file.path, &new_file_path).map_err(|_| libc::EIO)?;
        let old_fs_node = self.runtime_data.read().inodes[ino as usize];
        let new_fs_node =
            self.new_file_node_with_persistence(content_hash, size, Some(InodeNumber(ino)));
        self.update_directory_entry(
            open_file.parent_inode_number,
            &old_fs_node.calculate_hash(),
            new_fs_node.calculate_hash(),
        )?;
        self.runtime_data.write().inodes[ino as usize] = new_fs_node;
        Ok(())
    }

    pub fn pfs_lookup(&self, parent: u64, name: &str) -> Result<FileAttr, libc::c_int> {
        let (_fs_node, directory) = self.get_directory(parent)?;
        let runtime_data = self.runtime_data.read();
        for entry in directory.entries.iter() {
            if entry.name == name {
                let fs_node = &runtime_data.inodes[entry.inode_number.0 as usize];
                return Ok(fs_node.as_file_attr(entry.inode_number));
            }
        }
        Err(libc::ENOENT)
    }

    fn pfs_unlink(&mut self, parent: u64, name: &str) -> Result<(), libc::c_int> {
        let (_fs_node, directory) = self.get_directory(parent)?;
        let file_entry = directory
            .entries
            .iter()
            .find(|entry| entry.name == name)
            .ok_or(libc::ENOENT)?;
        let file_node_kind = {
            let runtime_data = self.runtime_data.read();
            runtime_data
                .inodes
                .get(file_entry.inode_number.0 as usize)
                .map(|node| node.kind)
                .ok_or(libc::ENOENT)?
        };
        if file_node_kind != FileType::RegularFile {
            return Err(libc::EISDIR);
        }
        self.remove_directory_entry(InodeNumber(parent), name)
    }

    fn pfs_rename(
        &mut self,
        parent: u64,
        name: &str,
        newparent: u64,
        newname: &str,
        no_replace: bool,
    ) -> Result<(), libc::c_int> {
        if parent == newparent && name == newname {
            return Ok(());
        }
        let (_source_fs_node, source_directory) = self.get_directory(parent)?;
        let source_entry = source_directory
            .entries
            .iter()
            .find(|entry| entry.name == name)
            .ok_or(libc::ENOENT)?
            .clone();
        let (_dest_fs_node, dest_directory) = self.get_directory(newparent)?;
        if no_replace
            && dest_directory
                .entries
                .iter()
                .any(|entry| entry.name == newname)
        {
            return Err(libc::EEXIST);
        }
        if parent != newparent {
            let mut runtime_data = self.runtime_data.write();
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
        self.add_directory_entry(InodeNumber(newparent), new_entry)?;
        self.remove_directory_entry(InodeNumber(parent), name)?;
        Ok(())
    }
}

impl Drop for Pfs {
    fn drop(&mut self) {
        self.persistence
            .persist_all()
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

        match self.pfs_getattr(ino) {
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
        match self.pfs_readdir(ino, offset) {
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

        match self.pfs_mkdir(parent, name_str) {
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

        match self.pfs_create(parent, name_str, flags) {
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
        match self.pfs_open(_ino, _flags) {
            Ok(fh) => reply.opened(fh, 0),
            Err(error) => {
                warn!("open failed for ino {}: {}", _ino, error);
                if error == libc::ENOENT {
                    if let Some(ref net) = self.network_communication {
                        let content_hash =
                            self.runtime_data.read().inodes[_ino as usize].content_hash;
                        debug!(
                            "File {} is not present locally, requesting it from peers",
                            content_hash
                        );
                        let mut pfs = self.clone();
                        net.request_file_with_callback(
                            content_hash,
                            Duration::from_secs(10),
                            Box::new(move || {
                                debug!("It seems like we received the file {} from a peer, try opening it again", content_hash);
                                match pfs.pfs_open(_ino, _flags) {
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
        match self.pfs_write(_ino, fh, offset, data) {
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
        match self.pfs_read(_ino, fh, offset, size) {
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
        match self.pfs_release(_ino, fh) {
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

        match self.pfs_lookup(parent, name_str) {
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

        match self.pfs_unlink(parent, file_name) {
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

        match self.pfs_rename(parent, old_name, newparent, new_name, no_replace) {
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
