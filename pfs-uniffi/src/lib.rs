// UniFFI bindings for PFS - exposes a simplified API for Kotlin/Swift/Python
// This is a separate library target that wraps the main PFS functionality

use pfs::network::IrohNetworkCommunication;
use pfs::Pfs;
use std::sync::Arc;

uniffi::setup_scaffolding!();

/// A simplified PFS client for use from foreign language bindings
#[derive(uniffi::Object)]
pub struct PfsClient {
    #[allow(dead_code)]
    pfs: Pfs,
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
    pub fn new(data_dir: String, peer_node_ids: Vec<String>) -> Result<Self, PfsError> {
        // Create data directory if it doesn't exist
        std::fs::create_dir_all(&data_dir)?;
        std::fs::create_dir_all(format!("{}/tmp", data_dir))?;

        // Build network communication using tokio runtime
        let runtime = tokio::runtime::Runtime::new().map_err(|e| {
            PfsError::InitializationError {
                error_message: format!("Failed to create tokio runtime: {}", e),
            }
        })?;

        let network_communication = runtime
            .block_on(async {
                IrohNetworkCommunication::build(pfs::config::new_secret_key())
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

        Ok(PfsClient { pfs, runtime })
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
