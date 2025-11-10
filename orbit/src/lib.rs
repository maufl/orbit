use std::{collections::BTreeMap, fmt::Display, fs::File, path::PathBuf, sync::Arc};

use network::{Messages, NetworkCommunication};
use parking_lot::RwLock;

use chrono::{DateTime, Utc};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};

use crate::persistence::{OrbitFsPersitence, Persistence};

pub mod config;
#[cfg(feature = "fuse")]
pub mod fuse;
pub mod network;
pub mod persistence;

#[cfg(feature = "test-utils")]
pub mod test_utils;

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

#[derive(Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Clone, Copy)]
pub struct BlockHash(pub [u8; 32]);

impl Display for BlockHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "b-{}", hex::encode(self.0))
    }
}

impl std::fmt::Debug for BlockHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy)]
pub struct Block {
    root_node_hash: FsNodeHash,
    previous_blocks: (BlockHash, Option<BlockHash>),
}

impl Block {
    pub fn calculate_hash(&self) -> BlockHash {
        BlockHash(calculate_hash(&self))
    }

    pub fn new(root_node_hash: FsNodeHash, previous_block_hash: BlockHash) -> Self {
        Block {
            root_node_hash,
            previous_blocks: (previous_block_hash, None),
        }
    }
}

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

    pub fn entry_by_name(&self, name: &str) -> Option<&RuntimeDirectoryEntry> {
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

pub(crate) struct OpenFile {
    pub(crate) path: PathBuf,
    pub(crate) backing_file: File,
    pub(crate) parent_inode_number: InodeNumber,
    pub(crate) writable: bool,
}

#[cfg(feature = "fuse")]
#[derive(Debug, Clone)]
pub struct RuntimeDirectoryEntryInfo {
    pub ino: u64,
    pub offset: i64,
    pub file_type: fuser::FileType,
    pub name: String,
}

pub struct OrbitFsRuntimeData {
    pub directories: BTreeMap<ContentHash, RuntimeDirectory>,
    pub inodes: Vec<RuntimeFsNode>,
    pub open_files: Vec<Option<OpenFile>>,
    pub current_block: Block,
}

#[derive(Clone)]
pub struct OrbitFs {
    pub runtime_data: Arc<RwLock<OrbitFsRuntimeData>>,
    pub data_dir: String,
    pub persistence: Arc<dyn Persistence>,
    network_communication: Option<Arc<dyn NetworkCommunication>>,
}

impl OrbitFs {
    pub fn initialize(
        data_dir: String,
        network_communication: Option<Arc<dyn NetworkCommunication>>,
    ) -> Result<OrbitFs, Box<dyn std::error::Error>> {
        let persistence = Arc::new(OrbitFsPersitence::new(&data_dir)?);

        let mut orbit_fs = OrbitFs {
            data_dir: data_dir.clone(),
            runtime_data: Arc::new(RwLock::new(OrbitFsRuntimeData {
                directories: BTreeMap::new(),
                inodes: vec![RuntimeFsNode::default(); 1],
                open_files: Vec::new(),
                current_block: Block::default(),
            })),
            persistence,
            network_communication,
        };

        // Try to load existing data
        if let Err(e) = orbit_fs.restore_filesystem_tree() {
            warn!("Failed to load persisted data, starting fresh: {}", e);
            // Start fresh if loading fails
            {
                let mut runtime_data = orbit_fs.runtime_data.write();
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
            orbit_fs
                .persistence
                .persist_directory(&(&initial_directory).into())?;
            orbit_fs
                .persistence
                .persist_fs_node(&initial_root_node.into())?;
            orbit_fs
                .runtime_data
                .write()
                .directories
                .insert(initial_root_node.content_hash, initial_directory);
            orbit_fs.assign_inode_number(initial_root_node.clone());

            // Create and persist the initial block
            let initial_block =
                Block::new(initial_root_node.calculate_hash(), BlockHash::default());
            if let Err(e) = orbit_fs.persistence.persist_block(&initial_block) {
                error!("Failed to persist initial block: {}", e);
            }
            orbit_fs.runtime_data.write().current_block = initial_block;
        }

        Ok(orbit_fs)
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
            // This is the root - create and persist a new block
            let old_block = self.runtime_data.read().current_block.clone();
            let new_block = Block::new(new_dir_node.calculate_hash(), old_block.calculate_hash());
            self.persistence.persist_block(&new_block)?;
            self.runtime_data.write().current_block = new_block;
        }

        Ok(())
    }

    fn restore_filesystem_tree(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Load the current block first
        let current_block = self.persistence.load_current_block()?;
        info!(
            "Found current block {} in persistent storage",
            current_block.calculate_hash()
        );

        // Extract the root hash from the block
        let root_hash = current_block.root_node_hash;
        info!("Root hash from block is {}", root_hash);

        // Restore the filesystem tree recursively, starting from the root
        let root_inode = self.restore_node_recursive(&root_hash, None)?;
        info!("Restored root node to inode {}", root_inode.0);

        // Update the runtime data with the current block
        self.runtime_data.write().current_block = current_block;

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

        // Check if an entry with the same name already exists
        if directory.entry_by_name(&new_entry.name).is_some() {
            return Err(libc::EEXIST);
        }

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
            // This is the root directory - create and persist a new block
            let old_block = self.runtime_data.read().current_block.clone();
            let new_block = Block::new(
                new_directory_node.calculate_hash(),
                old_block.calculate_hash(),
            );
            if let Err(e) = self.persistence.persist_block(&new_block) {
                error!("Failed to persist block: {}", e);
            }
            self.runtime_data.write().current_block = new_block;
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

        // Send the new block
        let current_block = self.runtime_data.read().current_block.clone();
        network_comm.send_message(Messages::BlockChanged(current_block));
    }
}

impl Drop for OrbitFs {
    fn drop(&mut self) {
        self.persistence
            .persist_all()
            .expect("To be able to persist metadata")
    }
}

/// Wrapper struct providing path-based access to OrbitFs
pub struct OrbitFsWrapper {
    orbit_fs: OrbitFs,
}

impl OrbitFsWrapper {
    pub fn new(orbit_fs: OrbitFs) -> Self {
        OrbitFsWrapper { orbit_fs }
    }

    /// Traverse the filesystem tree to find a node by path
    /// Returns the RuntimeFsNode for further processing
    fn traverse_to_node(&self, path: &str) -> Result<RuntimeFsNode, String> {
        // Normalize path - remove leading/trailing slashes and split
        let path = path.trim_matches('/');

        // Handle root directory
        if path.is_empty() {
            return Ok(self.orbit_fs.get_root_node());
        }

        let components: Vec<&str> = path.split('/').collect();

        // Start from root
        let mut current_node = self.orbit_fs.get_root_node();

        // Traverse each path component
        for component in components {
            // Current node must be a directory
            if !current_node.is_directory() {
                return Err(format!("Not a directory: {}", path));
            }

            // Get the directory contents
            let runtime_data = self.orbit_fs.runtime_data.read();
            let directory = runtime_data
                .directories
                .get(&current_node.content_hash)
                .ok_or_else(|| format!("Path not found: {}", path))?;

            // Find the entry with matching name
            let entry = directory
                .entry_by_name(component)
                .ok_or_else(|| format!("Path not found: {}", path))?;

            // Get the node for this entry
            current_node = runtime_data
                .inodes
                .get(entry.inode_number.0 as usize)
                .copied()
                .ok_or_else(|| format!("Path not found: {}", path))?;
        }

        Ok(current_node)
    }

    /// Get a filesystem node by its path
    /// Recursively traverses from root following path components
    pub fn get_node_by_path(&self, path: &str) -> Result<FsNode, String> {
        let runtime_node = self.traverse_to_node(path)?;
        Ok(runtime_node.into())
    }

    /// List entries in a directory given by path
    pub fn list_directory(&self, path: &str) -> Result<Vec<(String, FsNode)>, String> {
        let dir_node = self.traverse_to_node(path)?;

        // Verify it's a directory
        if !dir_node.is_directory() {
            return Err(format!("Not a directory: {}", path));
        }

        // Get directory contents
        let runtime_data = self.orbit_fs.runtime_data.read();
        let directory = runtime_data
            .directories
            .get(&dir_node.content_hash)
            .ok_or_else(|| format!("Not a directory: {}", path))?;

        // Convert entries to the result format
        let entries: Vec<(String, FsNode)> = directory
            .entries
            .iter()
            .filter_map(|entry| {
                runtime_data
                    .inodes
                    .get(entry.inode_number.0 as usize)
                    .map(|node| (entry.name.clone(), (*node).into()))
            })
            .collect();

        Ok(entries)
    }

    /// Create a new empty file with the given name in the parent directory
    /// Returns the content hash of the empty file
    pub fn create_file(
        &mut self,
        parent_path: &str,
        filename: &str,
    ) -> Result<ContentHash, String> {
        use base64::Engine;

        // Validate filename
        if filename.is_empty() {
            return Err("Filename cannot be empty".to_string());
        }
        if filename.contains('/') {
            return Err("Filename cannot contain '/'".to_string());
        }

        // Get parent directory node
        let parent_node = self.traverse_to_node(parent_path)?;
        if !parent_node.is_directory() {
            return Err(format!("Parent is not a directory: {}", parent_path));
        }

        // Get parent inode number
        let parent_inode = {
            let runtime_data = self.orbit_fs.runtime_data.read();
            runtime_data
                .inodes
                .iter()
                .position(|n| n.content_hash == parent_node.content_hash)
                .map(|idx| InodeNumber(idx as u64))
                .ok_or_else(|| "Parent inode not found".to_string())?
        };

        // Create empty file content (hash of empty data)
        let empty_content_hash = ContentHash(hmac_sha256::Hash::hash(&[]));

        // Create file in the data directory with base64-encoded hash
        let file_path = format!(
            "{}/{}",
            self.orbit_fs.data_dir,
            base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(empty_content_hash.0)
        );
        std::fs::write(&file_path, &[]).map_err(|e| format!("Failed to create file: {}", e))?;

        // Create FsNode for the new file
        let file_node =
            self.orbit_fs
                .new_file_node_with_persistence(empty_content_hash, 0, Some(parent_inode));
        let file_node_hash = file_node.calculate_hash();

        // Add entry to parent directory
        let new_entry = RuntimeDirectoryEntry {
            name: filename.to_string(),
            fs_node_hash: file_node_hash,
            inode_number: self.orbit_fs.assign_inode_number(file_node),
        };

        self.orbit_fs
            .add_directory_entry(parent_inode, new_entry)
            .map_err(|e| {
                if e == libc::EEXIST {
                    format!("File already exists: {}", filename)
                } else {
                    format!("Failed to add directory entry: {}", e)
                }
            })?;

        Ok(empty_content_hash)
    }

    /// Get the backing file path for a given Orbit file path
    /// Returns the absolute path to the content file on disk
    pub fn get_backing_file_path(&self, path: &str) -> Result<String, String> {
        use base64::Engine;

        let node = self.traverse_to_node(path)?;

        // Only regular files have backing files
        if node.is_directory() {
            return Err(format!("Path is a directory: {}", path));
        }

        // Construct the backing file path from content hash (base64-encoded)
        let backing_path = format!(
            "{}/{}",
            self.orbit_fs.data_dir,
            base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(node.content_hash.0)
        );

        Ok(backing_path)
    }

    /// Update a file's content from a source file
    /// Reads the source file, calculates its hash, moves it to the data directory,
    /// and updates the filesystem metadata
    pub fn update_file_from(&mut self, file_path: &str, source_path: &str) -> Result<(), String> {
        use base64::Engine;
        use std::io::Read;

        // Get the current file node
        let current_node = self.traverse_to_node(file_path)?;
        if current_node.is_directory() {
            return Err(format!("Path is a directory: {}", file_path));
        }

        // Read the source file to calculate hash and size
        let mut file = std::fs::File::open(source_path)
            .map_err(|e| format!("Failed to open source file: {}", e))?;

        let mut size = 0u64;
        let mut buf = [0u8; 1024];
        let mut hasher = hmac_sha256::Hash::new();
        while let Ok(n) = file.read(&mut buf) {
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            size += n as u64;
        }
        let new_content_hash = ContentHash(hasher.finalize());

        // Move the source file to the data directory with base64-encoded content hash as filename
        let dest_path = format!(
            "{}/{}",
            self.orbit_fs.data_dir,
            base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(new_content_hash.0)
        );
        std::fs::rename(source_path, &dest_path)
            .or_else(|_| std::fs::copy(source_path, &dest_path).map(|_| ()))
            .map_err(|e| format!("Failed to move file: {}", e))?;

        // Get the current inode number
        let current_inode = {
            let runtime_data = self.orbit_fs.runtime_data.read();
            runtime_data
                .inodes
                .iter()
                .position(|n| {
                    n.content_hash == current_node.content_hash && n.kind == current_node.kind
                })
                .map(|idx| InodeNumber(idx as u64))
                .ok_or_else(|| "Current inode not found".to_string())?
        };

        // Get the old node hash before creating the new one
        let old_node_hash = current_node.calculate_hash();

        // Create new FsNode with updated content
        let new_file_node = self.orbit_fs.new_file_node_with_persistence(
            new_content_hash,
            size,
            current_node.parent_inode_number,
        );
        let new_file_node_hash = new_file_node.calculate_hash();

        // Update the directory entry first
        if let Some(parent_inode) = current_node.parent_inode_number {
            self.orbit_fs
                .update_directory_entry(parent_inode, &old_node_hash, new_file_node_hash)
                .map_err(|e| format!("Failed to update directory entry: {}", e))?;
        } else {
            return Err("File has no parent directory".to_string());
        }

        // Update the inode in place
        self.orbit_fs.runtime_data.write().inodes[current_inode.0 as usize] = new_file_node;

        Ok(())
    }
}
