// UniFFI bindings for PFS - exposes a simplified API for Kotlin/Swift/Python
// This is a separate library target that wraps the main PFS functionality

use pfs::network::IrohNetworkCommunication;
use pfs::Pfs;
use std::sync::Arc;
use log::info;

uniffi::setup_scaffolding!();

#[derive(uniffi::Record, Clone, Debug)]
pub struct Config {
    /// Private key for this node (hex encoded)
    pub private_key: Option<String>,
    /// Directory to store filesystem data
    pub data_dir: String,
    /// List of peer node IDs to connect to on startup (hex encoded)
    pub peer_node_ids: Vec<String>,
}

/// A simplified PFS client for use from foreign language bindings
#[derive(uniffi::Object)]
pub struct PfsClient {
    #[allow(dead_code)]
    pfs: Pfs,
    config: Config,
    #[allow(dead_code)]
    runtime: tokio::runtime::Runtime,
}

#[uniffi::export]
impl PfsClient {
    /// Create a new PFS client with the given data directory and optional peer node IDs
    ///
    /// This initializes the filesystem with network communication enabled.
    /// The data directory will be created if it doesn't exist.
    /// A secret key will be generated and persisted if one doesn't exist.
    ///
    /// # Arguments
    /// * `data_dir` - Directory to store filesystem data
    /// * `peer_node_ids` - Optional list of peer node IDs to connect to (hex format)
    #[uniffi::constructor]
    pub fn new(config: Config) -> Result<Self, PfsError> {
        // Initialize Android logger
        #[cfg(target_os = "android")]
        {
            android_logger::init_once(
                android_logger::Config::default()
                    .with_max_level(log::LevelFilter::Debug)
                    .with_tag("PFS")
                    .with_filter(
                        android_logger::FilterBuilder::new()
                            .filter_module("pfs", log::LevelFilter::Debug)
                            .filter_module("pfs_uniffi", log::LevelFilter::Debug)
                            .build()
                    )
            );
        }
        let data_dir = &config.data_dir;

        // Create data directory if it doesn't exist
        std::fs::create_dir_all(data_dir)?;
        std::fs::create_dir_all(format!("{}/tmp", data_dir))?;

        // Build network communication using tokio runtime
        let runtime = tokio::runtime::Runtime::new().map_err(|e| {
            PfsError::InitializationError {
                error_message: format!("Failed to create tokio runtime: {}", e),
            }
        })?;

        let secret_key = if let Some(private_key) = &config.private_key {
            pfs::config::decode_secret_key(&private_key).map_err(|e| PfsError::InitializationError { error_message: format!("Failed to decode private key: {}", e) })?
        } else {
            pfs::config::new_secret_key()
        };
        info!("The node ID is {}", hex::encode(secret_key.public().as_bytes()));

        // Update config with the secret key (either the one provided or newly generated)
        let mut final_config = config.clone();
        if final_config.private_key.is_none() {
            final_config.private_key = Some(pfs::config::encode_secret_key(&secret_key));
        }

        let network_communication = runtime
            .block_on(async {
                IrohNetworkCommunication::build(secret_key)
                    .await
                    .map_err(|e| PfsError::InitializationError {
                        error_message: format!("Failed to build network communication: {}", e),
                    })
            })?;

        let network_communication = Arc::new(network_communication);

        // Initialize PFS
        let pfs = Pfs::initialize(data_dir.clone(), Some(network_communication.clone()))
            .map_err(|e| PfsError::InitializationError {
                error_message: format!("Failed to initialize PFS: {}", e),
            })?;

        Ok(PfsClient { pfs, config: final_config, runtime })
    }

    pub fn get_config(&self) -> Config {
        self.config.clone()
    }
}

/// Errors that can occur in PFS operations
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum PfsError {
    #[error("IO error: {error_message}")]
    IoError { error_message: String },

    #[error("Initialization error: {error_message       }")]
    InitializationError { error_message: String },
}

impl From<std::io::Error> for PfsError {
    fn from(err: std::io::Error) -> Self {
        PfsError::IoError {
            error_message: err.to_string(),
        }
    }
}
