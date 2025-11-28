use serde::{Deserialize, Serialize};

/// Information about a discovered peer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredPeer {
    /// Node ID (public key) as hex string
    pub node_id: String,
    /// Human-readable name of the peer
    pub name: String,
}

/// RPC service for interacting with the Orbit daemon
#[tarpc::service]
pub trait OrbitRpc {
    /// Get the node ID (public key) of this Orbit instance
    async fn get_node_id() -> String;

    /// Get list of peers discovered via mDNS
    async fn get_discovered_peers() -> Vec<DiscoveredPeer>;

    /// Add a peer to the known peers list and establish connection
    /// Returns Ok(()) if successful, Err(String) with error message otherwise
    async fn add_peer(node_id: String) -> Result<(), String>;
}
