use fjall::{Config, Keyspace, PartitionHandle};
use log::debug;

use crate::{ContentHash, Directory, FsNode, FsNodeHash};

pub trait Persistence: Send + Sync {
    fn load_fs_node(&self, node_hash: &FsNodeHash) -> Result<FsNode, anyhow::Error>;
    fn load_directory(&self, content_hash: &ContentHash) -> Result<Directory, anyhow::Error>;
    fn persist_fs_node(&self, fs_node: &FsNode) -> Result<(), anyhow::Error>;
    fn persist_directory(&self, directory: &Directory) -> Result<(), anyhow::Error>;
    fn persist_root_hash(&self, root_hash: &FsNodeHash) -> Result<(), anyhow::Error>;
    fn load_root_hash(&self) -> Result<FsNodeHash, anyhow::Error>;
    fn persist_all(&self) -> Result<(), anyhow::Error>;
}

pub struct PfsPersistence {
    /// This field may not be removed! It looks unused, but if you drop the Keyspace fjall will stop working
    keyspace: Keyspace,
    fs_nodes_partition: PartitionHandle,
    directories_partition: PartitionHandle,
}

impl PfsPersistence {
    pub fn new(data_dir: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let kv_path = format!("{}/metadata", data_dir);
        std::fs::create_dir_all(&kv_path)?;

        let keyspace = Config::new(&kv_path).open()?;
        let fs_nodes_partition = keyspace.open_partition("fs_nodes", Default::default())?;
        let directories_partition = keyspace.open_partition("directories", Default::default())?;

        Ok(PfsPersistence {
            keyspace,
            fs_nodes_partition,
            directories_partition,
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
        debug!("Persisted FsNode with hash {}", node_hash);
        Ok(())
    }

    fn persist_directory(&self, directory: &Directory) -> Result<(), anyhow::Error> {
        let content_hash = directory.calculate_hash();
        let key = content_hash.0;
        let mut value = Vec::new();
        ciborium::into_writer(directory, &mut value)?;
        self.directories_partition.insert(&key, &value)?;
        debug!("Persisted Directory with content hash {}", content_hash);
        Ok(())
    }

    fn persist_root_hash(&self, root_hash: &FsNodeHash) -> Result<(), anyhow::Error> {
        let key = b"__ROOT_HASH__";
        let mut value = Vec::new();
        ciborium::into_writer(root_hash, &mut value)?;
        self.fs_nodes_partition.insert(key, &value)?;
        debug!("Persisted root hash {}", root_hash);
        Ok(())
    }

    fn load_root_hash(&self) -> Result<FsNodeHash, anyhow::Error> {
        let key = b"__ROOT_HASH__";
        let Some(value) = self.fs_nodes_partition.get(key)? else {
            return Err(anyhow::anyhow!("No root hash found in metadata"));
        };
        let root_hash = ciborium::from_reader(&*value)?;
        Ok(root_hash)
    }

    fn persist_all(&self) -> Result<(), anyhow::Error> {
        self.keyspace.persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }
}
