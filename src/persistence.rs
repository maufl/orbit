use fjall::{Config, Keyspace, PartitionHandle};

use crate::{Block, BlockHash, ContentHash, Directory, FsNode, FsNodeHash};

pub trait Persistence: Send + Sync {
    fn load_fs_node(&self, node_hash: &FsNodeHash) -> Result<FsNode, anyhow::Error>;
    fn load_directory(&self, content_hash: &ContentHash) -> Result<Directory, anyhow::Error>;
    fn persist_fs_node(&self, fs_node: &FsNode) -> Result<(), anyhow::Error>;
    fn persist_directory(&self, directory: &Directory) -> Result<(), anyhow::Error>;
    fn persist_block(&self, block: &Block) -> Result<(), anyhow::Error>;
    fn load_current_block(&self) -> Result<Block, anyhow::Error>;
    fn load_block(&self, block_hash: &BlockHash) -> Result<Block, anyhow::Error>;
    fn persist_all(&self) -> Result<(), anyhow::Error>;
    fn diff(&self, old_root_node: &FsNode, new_root_node: &FsNode)
    -> (Vec<FsNode>, Vec<Directory>);
}

pub struct PfsPersistence {
    /// This field may not be removed! It looks unused, but if you drop the Keyspace fjall will stop working
    keyspace: Keyspace,
    fs_nodes_partition: PartitionHandle,
    directories_partition: PartitionHandle,
    blocks_partition: PartitionHandle,
}

impl PfsPersistence {
    pub fn new(data_dir: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let kv_path = format!("{}/metadata", data_dir);
        std::fs::create_dir_all(&kv_path)?;

        let keyspace = Config::new(&kv_path).open()?;
        let fs_nodes_partition = keyspace.open_partition("fs_nodes", Default::default())?;
        let directories_partition = keyspace.open_partition("directories", Default::default())?;
        let blocks_partition = keyspace.open_partition("blocks", Default::default())?;

        Ok(PfsPersistence {
            keyspace,
            fs_nodes_partition,
            directories_partition,
            blocks_partition,
        })
    }

    pub fn get_fs_nodes_partition(&self) -> &PartitionHandle {
        &self.fs_nodes_partition
    }
}

impl Persistence for PfsPersistence {
    fn load_fs_node(&self, node_hash: &FsNodeHash) -> Result<FsNode, anyhow::Error> {
        if let Some(value) = self.fs_nodes_partition.get(&node_hash.0)? {
            let fs_node: FsNode = ciborium::from_reader(&*value)?;
            return Ok(fs_node);
        }
        Err(anyhow::anyhow!("FsNode with hash {} not found", node_hash))
    }

    fn load_directory(&self, content_hash: &ContentHash) -> Result<Directory, anyhow::Error> {
        if let Some(value) = self.directories_partition.get(&content_hash.0)? {
            let directory: Directory = ciborium::from_reader(&*value)?;
            return Ok(directory);
        }
        Err(anyhow::anyhow!(
            "Directory with hash {} not found",
            content_hash
        ))
    }

    fn persist_fs_node(&self, fs_node: &FsNode) -> Result<(), anyhow::Error> {
        let node_hash = fs_node.calculate_hash();
        let key = node_hash.0;
        let mut value = Vec::new();
        ciborium::into_writer(fs_node, &mut value)?;
        self.fs_nodes_partition.insert(&key, &value)?;
        Ok(())
    }

    fn persist_directory(&self, directory: &Directory) -> Result<(), anyhow::Error> {
        let content_hash = directory.calculate_hash();
        let key = content_hash.0;
        let mut value = Vec::new();
        ciborium::into_writer(directory, &mut value)?;
        self.directories_partition.insert(&key, &value)?;
        Ok(())
    }

    fn persist_block(&self, block: &Block) -> Result<(), anyhow::Error> {
        let block_hash = block.calculate_hash();
        let key = block_hash.0;
        let mut value = Vec::new();
        ciborium::into_writer(block, &mut value)?;
        self.blocks_partition.insert(&key, &value)?;

        // Also update the current block pointer
        let current_key = b"__CURRENT_BLOCK__";
        let mut current_value = Vec::new();
        ciborium::into_writer(&block_hash, &mut current_value)?;
        self.blocks_partition.insert(current_key, &current_value)?;
        Ok(())
    }

    fn load_current_block(&self) -> Result<Block, anyhow::Error> {
        let key = b"__CURRENT_BLOCK__";
        let Some(value) = self.blocks_partition.get(key)? else {
            return Err(anyhow::anyhow!("No current block found in metadata"));
        };
        let block_hash: BlockHash = ciborium::from_reader(&*value)?;
        self.load_block(&block_hash)
    }

    fn load_block(&self, block_hash: &BlockHash) -> Result<Block, anyhow::Error> {
        if let Some(value) = self.blocks_partition.get(&block_hash.0)? {
            let block: Block = ciborium::from_reader(&*value)?;
            return Ok(block);
        }
        Err(anyhow::anyhow!("Block with hash {} not found", block_hash))
    }

    fn persist_all(&self) -> Result<(), anyhow::Error> {
        self.keyspace.persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }

    fn diff(
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
}

impl PfsPersistence {
    fn diff_recursive(
        &self,
        old_dir_node: &FsNode,
        new_dir_node: &FsNode,
        fs_nodes: &mut Vec<FsNode>,
        directories: &mut Vec<Directory>,
    ) {
        let old_directory = self.load_directory(&old_dir_node.content_hash).unwrap();
        let new_directory = self.load_directory(&new_dir_node.content_hash).unwrap();
        for entry in new_directory.entries.iter() {
            if !old_directory.entries.iter().any(|e| e.name == entry.name) {
                // New entry!
                let new_fs_node = self.load_fs_node(&entry.fs_node_hash).unwrap();
                if new_fs_node.is_directory() {
                    // Simplification, we assume that directories will always be created empty and that we don't need to recurse
                    let new_directory = self.load_directory(&new_fs_node.content_hash).unwrap();
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
                let changed_fs_node = self.load_fs_node(&entry.fs_node_hash).unwrap();
                if changed_fs_node.is_directory() {
                    let old_entry = old_directory
                        .entries
                        .iter()
                        .find(|e| e.name == entry.name)
                        .unwrap();
                    let old_fs_node = self
                        .load_fs_node(&old_entry.fs_node_hash)
                        .expect("To find old RuntimeFsNode");
                    self.diff_recursive(&old_fs_node, &changed_fs_node, fs_nodes, directories);
                } else {
                    fs_nodes.push(changed_fs_node);
                };
            }
        }
        fs_nodes.push(new_dir_node.clone());
        directories.push(new_directory);
    }
}
