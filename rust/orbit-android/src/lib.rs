// UniFFI bindings for Orbit - exposes a simplified API for Kotlin/Swift/Python
// This is a separate library target that wraps the main Orbit functionality

use log::info;
use orbit::network::{IrohNetworkCommunication, NetworkCommunication};
use orbit::{FileType, FsNode, OrbitFs, OrbitFsWrapper};
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;

uniffi::setup_scaffolding!();

#[derive(uniffi::Record, Clone, Debug)]
pub struct Config {
    /// Private key for this node (hex encoded)
    pub private_key: Option<String>,
    /// Directory to store filesystem data
    pub data_dir: String,
    /// List of peer node IDs to connect to on startup (hex encoded)
    pub peer_node_ids: Vec<String>,
    /// Optional human-readable name for this node (used for discovery)
    pub node_name: Option<String>,
}

/// A simplified Orbit client for use from foreign language bindings
#[derive(uniffi::Object)]
pub struct OrbitClient {
    fs_wrapper: RwLock<OrbitFsWrapper>,
    orbit_fs: OrbitFs,
    config: Config,
    network_communication: Arc<IrohNetworkCommunication>,
    #[allow(dead_code)]
    runtime: tokio::runtime::Runtime,
}

#[uniffi::export]
impl OrbitClient {
    /// Create a new Orbit client with the given data directory and optional peer node IDs
    ///
    /// This initializes the filesystem with network communication enabled.
    /// The data directory will be created if it doesn't exist.
    /// A secret key will be generated and persisted if one doesn't exist.
    #[uniffi::constructor]
    pub fn new(config: Config) -> Result<Self, OrbitError> {
        // Initialize Android logger
        #[cfg(target_os = "android")]
        {
            android_logger::init_once(
                android_logger::Config::default()
                    .with_max_level(log::LevelFilter::Debug)
                    .with_tag("Orbit")
                    .with_filter(
                        android_logger::FilterBuilder::new()
                            .filter_module("orbit", log::LevelFilter::Debug)
                            .filter_module("orbit_android", log::LevelFilter::Debug)
                            .build(),
                    ),
            );
        }
        let data_dir = &config.data_dir;

        // Create data directory if it doesn't exist
        std::fs::create_dir_all(data_dir)?;
        std::fs::create_dir_all(format!("{}/tmp", data_dir))?;

        // Build network communication using tokio runtime
        let runtime =
            tokio::runtime::Runtime::new().map_err(|e| OrbitError::InitializationError {
                error_message: format!("Failed to create tokio runtime: {}", e),
            })?;

        let secret_key = if let Some(private_key) = &config.private_key {
            orbit::config::decode_secret_key(&private_key).map_err(|e| {
                OrbitError::InitializationError {
                    error_message: format!("Failed to decode private key: {}", e),
                }
            })?
        } else {
            orbit::config::new_secret_key()
        };
        info!(
            "The node ID is {} {}",
            hex::encode(secret_key.public().as_bytes()),
            base32::encode(base32::Alphabet::Z, secret_key.public().as_bytes())
        );

        // Update config with the secret key (either the one provided or newly generated)
        let mut final_config = config.clone();
        if final_config.private_key.is_none() {
            final_config.private_key = Some(orbit::config::encode_secret_key(&secret_key));
        }

        let network_communication = runtime.block_on(async {
            IrohNetworkCommunication::build(secret_key, config.node_name.clone())
                .await
                .map_err(|e| OrbitError::InitializationError {
                    error_message: format!("Failed to build network communication: {}", e),
                })
        })?;

        let network_communication = Arc::new(network_communication);

        // Set known peers list from config
        network_communication.set_known_peers(config.peer_node_ids.clone());

        // Initialize Orbit
        let orbit_fs = OrbitFs::initialize(data_dir.clone(), Some(network_communication.clone()))
            .map_err(|e| OrbitError::InitializationError {
            error_message: format!("Failed to initialize Orbit: {}", e),
        })?;

        runtime.block_on(async {
            // accept incoming connections
            network_communication.accept_connections(orbit_fs.clone());
            // Connect to all peers from config
            network_communication
                .connect_to_all_peers(config.peer_node_ids.clone(), orbit_fs.clone())
                .await;
        });

        let fs_wrapper = OrbitFsWrapper::new(orbit_fs.clone());

        Ok(OrbitClient {
            fs_wrapper: RwLock::new(fs_wrapper),
            orbit_fs,
            config: final_config,
            network_communication,
            runtime,
        })
    }

    pub fn get_config(&self) -> Config {
        self.config.clone()
    }

    /// Get a filesystem node by its path
    /// Recursively traverses from root following path components
    pub fn get_node_by_path(&self, path: String) -> Result<FsNodeInfo, OrbitError> {
        let node = self
            .fs_wrapper
            .read()
            .get_node_by_path(&path)
            .map_err(|e| OrbitError::PathNotFound { path: e })?;
        Ok(FsNodeInfo::from_fs_node(&node))
    }

    /// List entries in a directory given by path
    pub fn list_directory(&self, path: String) -> Result<Vec<DirectoryEntryInfo>, OrbitError> {
        let entries = self
            .fs_wrapper
            .read()
            .list_directory(&path)
            .map_err(|e| OrbitError::PathNotFound { path: e })?;

        let result = entries
            .into_iter()
            .map(|(name, node)| DirectoryEntryInfo {
                name,
                kind: match node.kind {
                    FileType::Directory => FileKind::Directory,
                    FileType::RegularFile => FileKind::RegularFile,
                    FileType::Symlink => FileKind::Symlink,
                },
                size: node.size,
                modification_time_ms: node.modification_time.timestamp_millis(),
            })
            .collect();

        Ok(result)
    }

    /// Create a new empty file with the given name in the parent directory
    pub fn create_file(&self, parent_path: String, filename: String) -> Result<(), OrbitError> {
        self.fs_wrapper
            .write()
            .create_file(&parent_path, &filename)
            .map_err(|e| {
                if e.contains("File already exists") {
                    OrbitError::FileExists {
                        path: filename.clone(),
                    }
                } else if e.contains("not a directory") {
                    OrbitError::NotADirectory {
                        path: parent_path.clone(),
                    }
                } else {
                    OrbitError::PathNotFound { path: e }
                }
            })?;
        Ok(())
    }

    /// Get the backing file path for a given Orbit file path
    pub fn get_backing_file_path(&self, path: String) -> Result<String, OrbitError> {
        self.fs_wrapper
            .read()
            .get_backing_file_path(&path)
            .map_err(|e| OrbitError::PathNotFound { path: e })
    }

    /// Update a file's content from a source file
    pub fn update_file_from(
        &self,
        file_path: String,
        source_path: String,
    ) -> Result<(), OrbitError> {
        self.fs_wrapper
            .write()
            .update_file_from(&file_path, &source_path)
            .map_err(|e| OrbitError::PathNotFound { path: e })
    }

    /// Request a file's content from peers
    ///
    /// This is useful when a file's metadata is known but the content is not yet available locally.
    /// The callback will be invoked with the result after either:
    /// - The file is successfully received from a peer (Success)
    /// - The timeout expires without receiving the file (Timeout)
    ///
    /// # Arguments
    /// * `file_path` - Path to the file whose content should be requested
    /// * `timeout_seconds` - Maximum time to wait for the file (in seconds)
    /// * `callback` - Callback to invoke when the request completes
    pub fn request_file(
        &self,
        file_path: String,
        timeout_seconds: u64,
        callback: Arc<dyn FileRequestCallback>,
    ) -> Result<(), OrbitError> {
        // Get the file node to extract content hash
        let node = self
            .fs_wrapper
            .read()
            .get_node_by_path(&file_path)
            .map_err(|e| OrbitError::PathNotFound { path: e })?;

        // Only request regular files, not directories
        if node.kind == FileType::Directory {
            return Err(OrbitError::NotADirectory { path: file_path });
        }

        let content_hash = node.content_hash;
        let timeout = Duration::from_secs(timeout_seconds);

        // Use the NetworkCommunication trait method to request the file
        // Wrap the UniFFI callback to convert bool to FileRequestResult
        self.network_communication.request_file_with_callback(
            content_hash,
            timeout,
            Box::new(move |success| {
                let result = if success {
                    FileRequestResult::Success
                } else {
                    FileRequestResult::Timeout
                };
                callback.on_complete(result);
            }),
        );

        Ok(())
    }

    /// Register a callback to be called when the filesystem root changes
    ///
    /// This is useful for getting notified when files change due to network synchronization.
    /// The callback will be invoked on a background thread whenever the filesystem root is updated.
    ///
    /// # Arguments
    /// * `callback` - Callback to invoke when the root changes
    pub fn register_root_change_callback(&self, callback: Arc<dyn RootChangeCallback>) {
        self.fs_wrapper
            .read()
            .register_root_change_callback(Arc::new(move || {
                callback.on_root_changed();
            }));
    }

    /// Register a callback to be called when peers are discovered on the local network
    ///
    /// This uses mDNS to discover other Orbit nodes on the same local network.
    /// The callback will be invoked on a background thread whenever a peer is discovered or expires.
    ///
    /// # Arguments
    /// * `callback` - Callback to invoke when peers are discovered or expire
    pub fn register_peer_discovery_callback(&self, callback: Arc<dyn PeerDiscoveryCallback>) {
        let network_communication = self.network_communication.clone();

        info!("Registering peer discovery callback");

        // Spawn a task to subscribe to mDNS events
        self.runtime.spawn(async move {
            if let Some(mdns) = &network_communication.mdns_discovery {
                info!("mDNS discovery is enabled, subscribing to events");
                let mut events = mdns.subscribe().await;

                use futures::StreamExt;
                use orbit::network::MdnsDiscoveryEvent;

                info!("Waiting for mDNS discovery events...");
                while let Some(event) = events.next().await {
                    match event {
                        MdnsDiscoveryEvent::Discovered {
                            endpoint_info,
                            last_updated,
                        } => {
                            info!(
                                "Peer discovered via mDNS: {}",
                                hex::encode(endpoint_info.endpoint_id.as_bytes())
                            );

                            // Filter for Orbit nodes and extract the node name
                            let user_data = endpoint_info.data.user_data();
                            let node_name = if let Some(data) = user_data {
                                let data_str = data.as_ref();
                                if let Some(name) = data_str.strip_prefix("orbit:") {
                                    Some(name.to_string())
                                } else {
                                    info!("Discovered endpoint without 'orbit:' prefix, ignoring");
                                    continue;
                                }
                            } else {
                                info!("Discovered endpoint without user data, ignoring");
                                continue;
                            };

                            let peer_info = PeerInfo {
                                node_id: hex::encode(endpoint_info.endpoint_id.as_bytes()),
                                node_name,
                                last_seen_ms: last_updated.map(|ts| ts as i64),
                            };
                            callback.on_peer_discovered(peer_info);
                        }
                        MdnsDiscoveryEvent::Expired { endpoint_id } => {
                            let node_id = hex::encode(endpoint_id.as_bytes());
                            info!("Peer expired via mDNS: {}", node_id);
                            callback.on_peer_expired(node_id);
                        }
                    }
                }
            } else {
                info!("mDNS discovery is NOT enabled");
            }
        });
    }

    /// Add a peer to the known peers list and establish connection
    ///
    /// This will:
    /// 1. Validate the node ID format
    /// 2. Add the peer to the runtime known peers list
    /// 3. Send a PeerAdded control message to the peer
    /// 4. Establish an FS connection for synchronization
    ///
    /// Note: The caller is responsible for persisting the peer to SharedPreferences
    ///
    /// # Arguments
    /// * `node_id` - Hex-encoded node ID of the peer to add
    ///
    /// # Returns
    /// * `Ok(())` if successful
    /// * `Err(OrbitError)` if the node ID is invalid or already added
    pub fn add_peer(&self, node_id: String) -> Result<(), OrbitError> {
        info!("Adding peer: {}", node_id);

        // Validate node ID format
        hex::decode(&node_id).map_err(|_| OrbitError::InvalidPeerNodeId {
            node_id: node_id.clone(),
        })?;

        // Add peer to network communication's known peers list
        self.network_communication.add_known_peer(node_id.clone());

        let network_communication = self.network_communication.clone();
        let orbit_fs = self.orbit_fs.clone();

        // Send PeerAdded control message and establish FS connection if peer acknowledges
        self.runtime.spawn(async move {
            // Send PeerAdded control message and wait for acknowledgment
            match network_communication
                .send_peer_added_message(node_id.clone())
                .await
            {
                Ok(peer_added_us) => {
                    if peer_added_us {
                        log::info!(
                            "Peer {} has also added us, establishing FS connection",
                            node_id
                        );
                        // Establish FS connection now that both sides have confirmed
                        network_communication
                            .connect_to_all_peers(vec![node_id.clone()], orbit_fs)
                            .await;
                    } else {
                        log::info!(
                            "Peer {} has not added us yet, waiting for them to add us",
                            node_id
                        );
                    }
                }
                Err(e) => {
                    log::warn!("Failed to send PeerAdded message: {}", e);
                    // Don't return error here - peer is still added to known list
                }
            }
        });

        Ok(())
    }

    /// Get the list of known peers
    ///
    /// Returns the list of peer node IDs from the configuration
    pub fn get_known_peers(&self) -> Vec<String> {
        self.config.peer_node_ids.clone()
    }
}

/// Information about a filesystem node
#[derive(uniffi::Record, Clone, Debug)]
pub struct FsNodeInfo {
    pub size: u64,
    pub kind: FileKind,
    /// Modification time in milliseconds since Unix epoch
    pub modification_time_ms: i64,
}

impl FsNodeInfo {
    fn from_fs_node(node: &FsNode) -> Self {
        FsNodeInfo {
            size: node.size,
            kind: match node.kind {
                FileType::Directory => FileKind::Directory,
                FileType::RegularFile => FileKind::RegularFile,
                FileType::Symlink => FileKind::Symlink,
            },
            modification_time_ms: node.modification_time.timestamp_millis(),
        }
    }
}

/// File type enumeration for UniFFI
#[derive(uniffi::Enum, Clone, Debug, PartialEq)]
pub enum FileKind {
    Directory,
    RegularFile,
    Symlink,
}

/// Information about a directory entry
#[derive(uniffi::Record, Clone, Debug)]
pub struct DirectoryEntryInfo {
    pub name: String,
    pub kind: FileKind,
    pub size: u64,
    /// Modification time in milliseconds since Unix epoch
    pub modification_time_ms: i64,
}

/// Result of a file request operation
#[derive(uniffi::Enum, Clone, Debug)]
pub enum FileRequestResult {
    /// File was successfully received
    Success,
    /// Request timed out waiting for the file
    Timeout,
}

/// Callback trait for file request notifications
#[uniffi::export(with_foreign)]
pub trait FileRequestCallback: Send + Sync {
    /// Called when a file request completes (either success or timeout)
    fn on_complete(&self, result: FileRequestResult);
}

/// Callback trait for filesystem root change notifications
#[uniffi::export(with_foreign)]
pub trait RootChangeCallback: Send + Sync {
    /// Called when the filesystem root has changed
    fn on_root_changed(&self);
}

/// Information about a discovered peer
#[derive(uniffi::Record, Clone, Debug)]
pub struct PeerInfo {
    /// The node ID of the peer (hex encoded)
    pub node_id: String,
    /// Optional human-readable name of the peer
    pub node_name: Option<String>,
    /// Optional timestamp when this peer was last seen (milliseconds since epoch)
    pub last_seen_ms: Option<i64>,
}

/// Callback trait for peer discovery notifications
#[uniffi::export(with_foreign)]
pub trait PeerDiscoveryCallback: Send + Sync {
    /// Called when a peer is discovered on the local network
    fn on_peer_discovered(&self, peer: PeerInfo);
    /// Called when a peer expires (no longer advertising)
    fn on_peer_expired(&self, node_id: String);
}

/// Errors that can occur in Orbit operations
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum OrbitError {
    #[error("IO error: {error_message}")]
    IoError { error_message: String },

    #[error("Initialization error: {error_message}")]
    InitializationError { error_message: String },

    #[error("Path not found: {path}")]
    PathNotFound { path: String },

    #[error("Not a directory: {path}")]
    NotADirectory { path: String },

    #[error("File already exists: {path}")]
    FileExists { path: String },

    #[error("Invalid peer node ID: {node_id}")]
    InvalidPeerNodeId { node_id: String },
}

impl From<std::io::Error> for OrbitError {
    fn from(err: std::io::Error) -> Self {
        OrbitError::IoError {
            error_message: err.to_string(),
        }
    }
}
