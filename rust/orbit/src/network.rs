use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use crate::persistence::HistoryData;
use crate::{Block, BlockHash, ContentHash, OrbitFs};
use async_trait::async_trait;
use base64::Engine;
use bytes::{BufMut, Bytes, BytesMut};
use iroh::discovery::mdns::MdnsDiscovery;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::endpoint_info::UserData;
use iroh::{Endpoint, EndpointAddr, PublicKey, SecretKey};
use log::{debug, error, info, warn};
use parking_lot::RwLock;

// Re-export types needed by orbit-android
pub use iroh::discovery::mdns::DiscoveryEvent as MdnsDiscoveryEvent;
use serde::{Deserialize, Serialize};
use tokio::{
    runtime::Handle,
    sync::broadcast::{Receiver, Sender},
};

// ALPNs for different protocol types
pub const FS_ALPN: &str = "de.maufl.orbit.fs";
pub const CTRL_ALPN: &str = "de.maufl.orbit.ctrl";

// Filesystem synchronization messages
#[derive(Serialize, Deserialize, Clone)]
pub enum FsMessages {
    NotifyLatestBlock(Block),
    HistoryRequest((BlockHash, BlockHash)), // (start_inclusive, end_exclusive)
    NotifyHistory(HistoryData),
    ContentRequest(Vec<ContentHash>),
    ContentResponse((ContentHash, Bytes)),
}

// Control protocol messages for peer management
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum CtrlMessages {
    /// Sent when a peer is added to the known peers list
    PeerAdded,
    /// Acknowledgment of peer addition
    PeerAddedAck,
}

impl std::fmt::Debug for FsMessages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FsMessages::NotifyLatestBlock(block) => {
                f.debug_tuple("NotifyLatestBlock").field(block).finish()
            }
            FsMessages::HistoryRequest((start, end)) => f
                .debug_tuple("HistoryRequest")
                .field(&(start, end))
                .finish(),
            FsMessages::NotifyHistory(history) => {
                f.debug_tuple("NotifyHistory").field(history).finish()
            }
            FsMessages::ContentRequest(hashes) => f
                .debug_tuple("ContentRequest")
                .field(&format_args!("Vec<ContentHash>(len={})", hashes.len()))
                .finish(),
            FsMessages::ContentResponse((hash, bytes)) => f
                .debug_tuple("ContentResponse")
                .field(&(hash, format_args!("Bytes(len={})", bytes.len())))
                .finish(),
        }
    }
}

/// Trait for network communication capabilities
#[async_trait]
pub trait NetworkCommunication: Send + Sync {
    // Notify peers about changes to FS
    fn notify_latest_block(&self, history_data: HistoryData, block: Block);
    /// Request a file and execute a callback when it becomes available or times out
    /// The callback receives true if the file was received, false if it timed out
    fn request_file_with_callback(
        &self,
        content_hash: ContentHash,
        timeout: Duration,
        callback: Box<dyn FnOnce(bool) + Send>,
    );
}

/// Implementation of network communication using tokio broadcast channels
#[derive(Clone)]
pub struct IrohNetworkCommunication {
    fs_message_sender: Sender<FsMessages>,
    endpoint: Endpoint,
    #[allow(dead_code)] // Reserved for future use
    tokio_runtime_handle: Handle,
    content_notification_sender: Sender<ContentHash>,
    block_notification_sender: Sender<Vec<Block>>,
    pub mdns_discovery: Option<MdnsDiscovery>,
    /// Currently discovered peers via mDNS (node_id -> name)
    discovered_peers: Arc<RwLock<HashMap<String, String>>>,
    /// Known peers list (peers that have been explicitly added)
    known_peers: Arc<RwLock<Vec<String>>>,
}

impl IrohNetworkCommunication {
    pub async fn build(
        secret_key: SecretKey,
        node_name: Option<String>,
    ) -> anyhow::Result<IrohNetworkCommunication> {
        info!("Building endpoint for local network peer discovery");

        // Build endpoint first (without discovery)
        let endpoint = Endpoint::builder()
            .secret_key(secret_key)
            .alpns(vec![
                FS_ALPN.as_bytes().to_vec(),
                CTRL_ALPN.as_bytes().to_vec(),
            ])
            .bind()
            .await?;

        info!("Node ID is {}", endpoint.id());

        // Now create mDNS discovery with the endpoint's ID
        let mdns = MdnsDiscovery::builder().build(endpoint.id())?;
        info!("Created mDNS discovery service");

        // Add mDNS discovery to the endpoint
        endpoint.discovery().add(mdns.clone());
        info!("mDNS discovery added to endpoint");

        // Set user data to trigger publishing our endpoint info to mDNS
        // Use "orbit:" prefix followed by the node name to identify Orbit nodes
        let name = node_name.unwrap_or_else(|| "unnamed".to_string());
        let user_data_str = format!("orbit:{}", name);
        let user_data = UserData::try_from(user_data_str.clone())
            .map_err(|e| anyhow::anyhow!("Failed to create user data: {}", e))?;
        endpoint.set_user_data_for_discovery(Some(user_data));
        info!(
            "Triggered mDNS advertisement with name '{}' - now advertising and listening on local network",
            name
        );

        let (content_notification_sender, _content_notification_receiver) =
            tokio::sync::broadcast::channel(10);
        let (block_notification_sender, _block_notification_receiver) =
            tokio::sync::broadcast::channel(10);
        let (sender, _receiver) = tokio::sync::broadcast::channel(10);

        let discovered_peers = Arc::new(RwLock::new(HashMap::new()));
        let known_peers = Arc::new(RwLock::new(Vec::new()));

        // Start background task to update discovered peers from mDNS
        let mdns_clone = mdns.clone();
        let peers_clone = discovered_peers.clone();
        tokio::spawn(async move {
            Self::update_discovered_peers_task(mdns_clone, peers_clone).await;
        });

        Ok(IrohNetworkCommunication {
            fs_message_sender: sender.clone(),
            endpoint,
            tokio_runtime_handle: tokio::runtime::Handle::current(),
            content_notification_sender,
            block_notification_sender,
            mdns_discovery: Some(mdns),
            discovered_peers,
            known_peers,
        })
    }

    /// Set the known peers list (replaces existing list)
    pub fn set_known_peers(&self, peer_node_ids: Vec<String>) {
        let mut known_peers = self.known_peers.write();
        *known_peers = peer_node_ids;
    }

    /// Add a peer to the known peers list
    pub fn add_known_peer(&self, peer_node_id: String) {
        let mut known_peers = self.known_peers.write();
        if !known_peers.contains(&peer_node_id) {
            known_peers.push(peer_node_id);
        }
    }

    /// Check if a peer is in the known peers list
    fn is_known_peer(&self, peer_node_id: &str) -> bool {
        self.known_peers.read().contains(&peer_node_id.to_string())
    }

    pub async fn connect_to_all_peers(&self, peer_node_ids: Vec<String>, orbit_fs: OrbitFs) {
        if peer_node_ids.is_empty() {
            info!("No peer nodes configured, running in standalone mode");
            return;
        }

        info!("Connecting to {} peer(s)", peer_node_ids.len());

        for peer_node_id in peer_node_ids {
            if let Err(e) = self
                .connect_to_peer(peer_node_id.clone(), orbit_fs.clone())
                .await
            {
                warn!("Error connecting to peer {}: {}", peer_node_id, e)
            }
        }
    }

    pub fn accept_connections(&self, orbit_fs: OrbitFs) {
        let network = self.clone();
        tokio::spawn(async move {
            if let Err(e) = network.accept_connections_impl(orbit_fs).await {
                warn!("Error accepting connections: {}", e);
            }
        });
    }

    /// Background task that continuously listens to mDNS events and updates the discovered peers list
    async fn update_discovered_peers_task(
        mdns: MdnsDiscovery,
        peers: Arc<RwLock<HashMap<String, String>>>,
    ) {
        use futures::StreamExt;
        use iroh::discovery::mdns::DiscoveryEvent;

        let mut stream = mdns.subscribe().await;
        info!("Started mDNS peer discovery background task");

        while let Some(event) = stream.next().await {
            match event {
                DiscoveryEvent::Discovered { endpoint_info, .. } => {
                    let node_id = hex::encode(endpoint_info.endpoint_id.as_bytes());
                    let name = endpoint_info
                        .user_data()
                        .map(|data| data.as_ref())
                        .and_then(|s| s.strip_prefix("orbit:"))
                        .unwrap_or("unknown")
                        .to_string();

                    let mut peers_lock = peers.write();
                    if !peers_lock.contains_key(&node_id) {
                        peers_lock.insert(node_id.clone(), name.clone());
                        info!("Discovered peer: {} ({})", name, &node_id[..16]);
                    }
                }
                DiscoveryEvent::Expired { endpoint_id } => {
                    let node_id = hex::encode(endpoint_id.as_bytes());
                    if let Some(name) = peers.write().remove(&node_id) {
                        info!("Peer expired: {} ({})", name, &node_id[..16]);
                    }
                }
            }
        }

        warn!("mDNS peer discovery background task ended unexpectedly");
    }

    /// Get list of currently discovered peers via mDNS
    /// Returns a list of (node_id, name) tuples
    pub async fn get_discovered_peers(&self) -> Vec<(String, String)> {
        self.discovered_peers
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    async fn connect_to_peer(
        &self,
        peer_node_id: String,
        orbit_fs: OrbitFs,
    ) -> Result<(), anyhow::Error> {
        let remote_node_id = hex::decode(&peer_node_id).map_err(|e| {
            anyhow::anyhow!("Invalid hex format for node ID {}: {}", peer_node_id, e)
        })?;

        let mut remote_pub_key = [0u8; 32];
        remote_pub_key.copy_from_slice(&remote_node_id);

        let node_addr = EndpointAddr::new(PublicKey::from_bytes(&remote_pub_key)?);
        info!("Connecting to {}", node_addr.id);

        let network = self.clone();
        tokio::spawn(async move {
            let conn = loop {
                let node_addr = node_addr.clone();
                match network
                    .endpoint
                    .connect(node_addr, FS_ALPN.as_bytes())
                    .await
                {
                    Ok(conn) => break conn,
                    Err(e) => {
                        warn!("Failed to connect to node {}: {}", peer_node_id, e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    }
                }
            };
            if let Err(e) = network.open_fs_connection(conn, orbit_fs).await {
                warn!("Error while handling connection to {}: {}", peer_node_id, e);
            }
        });

        Ok(())
    }

    async fn accept_connections_impl(&self, orbit_fs: OrbitFs) -> Result<(), anyhow::Error> {
        while let Some(incoming) = self.endpoint.accept().await {
            if let Ok(conn) = incoming.await {
                let remote_id = conn.remote_id();
                let alpn = conn.alpn();
                info!(
                    "Accepted new connection from {} using ALPN {:?}",
                    remote_id, alpn
                );

                let orbit_fs_clone = orbit_fs.clone();
                let network = self.clone();

                // Handle connection based on ALPN
                let alpn_bytes: &[u8] = alpn.as_ref();
                if alpn_bytes == FS_ALPN.as_bytes() {
                    let remote_id_hex = hex::encode(remote_id.as_bytes());

                    // Check if peer is in known peers list
                    if !network.is_known_peer(&remote_id_hex) {
                        warn!(
                            "Rejecting FS connection from unknown peer: {}",
                            remote_id_hex
                        );
                        drop(conn);
                        continue;
                    }

                    tokio::spawn(async move {
                        if let Err(e) = network.accept_fs_connection(conn, orbit_fs_clone).await {
                            warn!("Error handling FS connection from {}: {}", remote_id, e);
                        }
                    });
                } else if alpn_bytes == CTRL_ALPN.as_bytes() {
                    tokio::spawn(async move {
                        if let Err(e) = network.handle_ctrl_connection(conn).await {
                            warn!(
                                "Error handling control connection from {}: {}",
                                remote_id, e
                            );
                        }
                    });
                } else {
                    warn!("Received connection with unknown ALPN: {:?}", alpn);
                }
            } else {
                warn!("Failed to accept incoming connection");
            }
        }
        Ok(())
    }

    async fn open_fs_connection(
        &self,
        conn: Connection,
        orbit_fs: OrbitFs,
    ) -> Result<(), anyhow::Error> {
        let (net_sender, net_receiver) = conn.open_bi().await?;
        info!("Opened new FS connection to {}", conn.remote_id());
        self.handle_fs_connection(net_sender, net_receiver, orbit_fs)
            .await
    }

    async fn accept_fs_connection(
        &self,
        conn: Connection,
        orbit_fs: OrbitFs,
    ) -> Result<(), anyhow::Error> {
        let (net_sender, net_receiver) = conn.accept_bi().await?;
        info!("Accepted FS connection from {}", conn.remote_id());
        self.handle_fs_connection(net_sender, net_receiver, orbit_fs)
            .await
    }

    async fn handle_fs_connection(
        &self,
        net_sender: SendStream,
        net_receiver: RecvStream,
        orbit_fs: OrbitFs,
    ) -> Result<(), anyhow::Error> {
        let receiver = self.fs_message_sender.subscribe();
        let current_block = orbit_fs.runtime_data.read().current_block.clone();
        if let Err(e) = self
            .fs_message_sender
            .send(FsMessages::NotifyLatestBlock(current_block))
        {
            warn!(
                "Unable to queue latest block notification for new connection: {}",
                e
            );
        };
        let network = self.clone();
        tokio::spawn(async move {
            network
                .listen_for_fs_updates(net_receiver, orbit_fs)
                .await
                .inspect_err(|e| warn!("Stopped listening for FS updates, encountered error {}", e))
        });
        let network = self.clone();
        tokio::spawn(async move {
            network
                .forward_fs_messages(net_sender, receiver)
                .await
                .inspect_err(|e| warn!("Stopped sending FS messages, encountered error {}", e))
        });
        Ok(())
    }

    async fn handle_ctrl_connection(&self, conn: Connection) -> Result<(), anyhow::Error> {
        let remote_id = conn.remote_id();
        info!("Handling control connection from {}", remote_id);
        let (mut net_sender, mut net_receiver) = conn.accept_bi().await?;

        // Read control message
        use bytes::Buf;
        let mut buffer = BytesMut::new();
        while let Some(chunk) = net_receiver.read_chunk(1024, true).await? {
            buffer.extend_from_slice(&chunk.bytes);

            let mut reader = buffer.clone().freeze().reader();
            match ciborium::from_reader::<CtrlMessages, _>(&mut reader) {
                Ok(msg) => {
                    info!("Received control message: {:?} from {}", msg, remote_id);
                    match msg {
                        CtrlMessages::PeerAdded => {
                            info!("Peer {} added us to their peers list", remote_id);

                            // Check if this peer is in our known peers list
                            let remote_id_hex = hex::encode(remote_id.as_bytes());
                            if self.is_known_peer(&remote_id_hex) {
                                info!(
                                    "Peer {} is in our known peers list, sending acknowledgment",
                                    remote_id_hex
                                );
                                // Send acknowledgment - this tells them we've also added them
                                let ack = serialize_ctrl_message(&CtrlMessages::PeerAddedAck);
                                if let Err(e) = net_sender.write_all(&ack).await {
                                    warn!("Failed to send PeerAddedAck: {}", e);
                                }
                            } else {
                                info!(
                                    "Peer {} not in our known peers list, NOT sending acknowledgment",
                                    remote_id_hex
                                );
                                // Don't send Ack - this tells them we haven't added them yet
                            }
                        }
                        CtrlMessages::PeerAddedAck => {
                            info!("Peer {} acknowledged peer addition", remote_id);
                        }
                    }
                    break;
                }
                Err(_) => {
                    // Wait for more data
                    continue;
                }
            }
        }

        Ok(())
    }

    /// Send a control message to a peer and wait for acknowledgment
    /// Returns true if the peer acknowledged (meaning they also added us)
    pub async fn send_peer_added_message(
        &self,
        peer_node_id: String,
    ) -> Result<bool, anyhow::Error> {
        let remote_node_id = hex::decode(&peer_node_id).map_err(|e| {
            anyhow::anyhow!("Invalid hex format for node ID {}: {}", peer_node_id, e)
        })?;

        let mut remote_pub_key = [0u8; 32];
        remote_pub_key.copy_from_slice(&remote_node_id);

        let node_addr = EndpointAddr::new(PublicKey::from_bytes(&remote_pub_key)?);
        info!("Sending PeerAdded message to {}", node_addr.id);

        // Connect using control ALPN
        let conn = self
            .endpoint
            .connect(node_addr, CTRL_ALPN.as_bytes())
            .await?;
        let (mut send, mut recv) = conn.open_bi().await?;

        // Send PeerAdded message
        let msg = serialize_ctrl_message(&CtrlMessages::PeerAdded);
        send.write_all(&msg).await?;
        send.finish()?;

        // Wait for acknowledgment with timeout
        use bytes::Buf;
        let mut buffer = BytesMut::new();
        let mut peer_added_us = false;

        // Set a 5 second timeout for receiving acknowledgment
        let timeout = tokio::time::Duration::from_secs(5);
        let timeout_future = tokio::time::sleep(timeout);
        tokio::pin!(timeout_future);

        loop {
            tokio::select! {
                _ = &mut timeout_future => {
                    warn!("Timeout waiting for PeerAddedAck from {}", peer_node_id);
                    break;
                }
                chunk_result = recv.read_chunk(1024, true) => {
                    match chunk_result? {
                        Some(chunk) => {
                            buffer.extend_from_slice(&chunk.bytes);

                            let mut reader = buffer.clone().freeze().reader();
                            match ciborium::from_reader::<CtrlMessages, _>(&mut reader) {
                                Ok(CtrlMessages::PeerAddedAck) => {
                                    info!("Received PeerAddedAck from {}", peer_node_id);
                                    peer_added_us = true;
                                    break;
                                }
                                Ok(msg) => {
                                    warn!("Unexpected control message: {:?}", msg);
                                }
                                Err(_) => {
                                    continue;
                                }
                            }
                        }
                        None => {
                            // Stream ended without acknowledgment
                            break;
                        }
                    }
                }
            }
        }

        Ok(peer_added_us)
    }

    fn process_received_fs_message(&self, msg: FsMessages, orbit_fs: &mut OrbitFs) {
        match msg {
            FsMessages::NotifyHistory(history) => {
                debug!(
                    "Received NotifyHistory with {} blocks, {} fs_nodes, {} directories",
                    history.blocks.len(),
                    history.fs_nodes.len(),
                    history.directories.len()
                );

                // Persist all directories
                for dir in &history.directories {
                    if let Err(e) = orbit_fs.persistence.persist_directory(dir) {
                        warn!("Unable to persist directory: {}", e);
                    }
                }

                // Persist all fs nodes
                for node in &history.fs_nodes {
                    if let Err(e) = orbit_fs.persistence.persist_fs_node(node) {
                        warn!("Unable to persist FS node: {}", e);
                    }
                }

                // Persist all blocks
                for block in &history.blocks {
                    if let Err(e) = orbit_fs.persistence.persist_block(block) {
                        warn!("Unable to persist block: {}", e);
                    }
                }

                // Notify that blocks are available
                if let Err(e) = self.block_notification_sender.send(history.blocks) {
                    warn!("Failed to notify about received blocks: {}", e);
                }
            }
            FsMessages::NotifyLatestBlock(remote_block) => {
                let remote_block_hash = remote_block.calculate_hash();
                debug!(
                    "Received NotifyLatestBlock message, remote block hash is {}",
                    remote_block_hash
                );

                // Get the current block and calculate hash
                let current_block = orbit_fs.runtime_data.read().current_block.clone();
                let current_block_hash = current_block.calculate_hash();

                // Persist the remote block first so ancestry checks can load it
                if let Err(e) = orbit_fs.persistence.persist_block(&remote_block) {
                    warn!("Unable to persist remote block: {}", e);
                    return;
                }

                // Check if the remote block is an ancestor of our current block
                // If so, the remote is behind us, send them the history they're missing
                if orbit_fs.is_block_ancestor(&remote_block_hash, &current_block_hash) {
                    info!(
                        "Remote block {} is an ancestor of our current block {}, sending them history",
                        remote_block_hash, current_block_hash
                    );

                    // Calculate the diff from our current block back to their block
                    let history = orbit_fs
                        .persistence
                        .diff(&current_block_hash, &remote_block_hash);

                    debug!(
                        "Sending NotifyHistory with {} blocks, {} fs_nodes, {} directories to catch up remote",
                        history.blocks.len(),
                        history.fs_nodes.len(),
                        history.directories.len()
                    );

                    if let Err(e) = self
                        .fs_message_sender
                        .send(FsMessages::NotifyHistory(history))
                    {
                        warn!("Failed to send history notification: {}", e);
                    }

                    return;
                }

                // Check if our current block is an ancestor of the remote block
                // If not, this indicates a divergence
                if !orbit_fs.is_block_ancestor(&current_block_hash, &remote_block_hash) {
                    warn!(
                        "Remote block {} is not a descendant of our current block {}, this indicates a divergence",
                        remote_block_hash, current_block_hash
                    );

                    // Try to find the common ancestor
                    if let Some(common_ancestor) =
                        orbit_fs.find_common_ancestor(&current_block_hash, &remote_block_hash)
                    {
                        info!(
                            "Found common ancestor block {} for diverged branches {} and {}",
                            common_ancestor, current_block_hash, remote_block_hash
                        );
                        // TODO: Implement three-way merge using the common ancestor
                        // For now, we'll skip the update to avoid potential data loss
                    } else {
                        warn!(
                            "Could not find common ancestor between {} and {}, blocks may be from different filesystems",
                            current_block_hash, remote_block_hash
                        );
                    }
                    return;
                }

                // Our current block is an ancestor of the remote block, proceed with update
                info!(
                    "Updating from block {} to block {}",
                    current_block_hash, remote_block_hash
                );

                // Request intermediate blocks if we're behind
                // This ensures we have all the necessary FsNodes and Directories
                if let Err(e) = self.fs_message_sender.send(FsMessages::HistoryRequest((
                    remote_block_hash,
                    current_block_hash,
                ))) {
                    warn!("Failed to send history request: {}", e);
                }

                if let Err(e) = orbit_fs.update_fs_root(current_block, remote_block) {
                    error!("Unable to update fs root: {}", e);
                } else {
                    info!(
                        "Successfully updated to new fs root: {}",
                        orbit_fs.get_root_node().calculate_hash()
                    );
                }
            }
            FsMessages::ContentRequest(requested_content) => {
                for content_hash in requested_content.into_iter() {
                    let file_path = orbit_fs.data_dir.clone()
                        + "/"
                        + &base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(content_hash.0);
                    let mut buf = BytesMut::new().writer();
                    let Ok(mut file) = std::fs::File::open(&file_path) else {
                        warn!("Failed to open file {}", file_path);
                        continue;
                    };
                    if let Err(e) = std::io::copy(&mut file, &mut buf) {
                        warn!("Failed to read file {}, {}", file_path, e);
                        continue;
                    };
                    debug!("Replying with content response");
                    if let Err(e) = self.fs_message_sender.send(FsMessages::ContentResponse((
                        content_hash,
                        buf.into_inner().freeze(),
                    ))) {
                        warn!("Failed to send content message: {}", e);
                    };
                }
            }
            FsMessages::ContentResponse((content_hash, bytes)) => {
                let new_file_path = orbit_fs.data_dir.clone()
                    + "/"
                    + &base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(content_hash.0);
                if let Err(e) = std::fs::write(&new_file_path, bytes.as_ref()) {
                    warn!(
                        "Failed to write content with hash {} to path {}: {}",
                        content_hash, new_file_path, e
                    );
                } else {
                    if let Err(e) = self.content_notification_sender.send(content_hash) {
                        warn!("Failed to notify about new content: {}", e)
                    }
                };
            }
            FsMessages::HistoryRequest((start_block, end_block)) => {
                debug!(
                    "Received HistoryRequest from {} to {}",
                    start_block, end_block
                );

                // Use persistence diff to get all blocks and changes
                let history = orbit_fs.persistence.diff(&start_block, &end_block);

                debug!(
                    "Sending NotifyHistory with {} blocks, {} fs_nodes, {} directories",
                    history.blocks.len(),
                    history.fs_nodes.len(),
                    history.directories.len()
                );
                if let Err(e) = self
                    .fs_message_sender
                    .send(FsMessages::NotifyHistory(history))
                {
                    warn!("Failed to send history notification: {}", e);
                }
            }
        }
    }

    async fn listen_for_fs_updates(
        &self,
        mut net_receiver: RecvStream,
        mut orbit_fs: OrbitFs,
    ) -> Result<(), anyhow::Error> {
        use bytes::Buf;

        let mut buffer = BytesMut::new();
        while let Some(chunk) = net_receiver.read_chunk(15_000, true).await? {
            // Add the new chunk to our buffer
            let mut writer = buffer.writer();
            writer.write_all(&chunk.bytes)?;
            buffer = writer.into_inner();
            debug!("Received chunk");

            // Try to deserialize as many complete messages as possible
            let mut current_buffer = buffer.freeze();
            loop {
                // Save the current buffer position for potential rollback (cheap clone)
                let buffer_before = current_buffer.clone();
                let mut reader = current_buffer.reader();

                match ciborium::from_reader::<FsMessages, _>(&mut reader) {
                    Ok(msg) => {
                        // Successfully deserialized a message - process it immediately
                        debug!("FS message is {:?}", msg);
                        current_buffer = reader.into_inner();

                        // Process the message using the extracted function
                        self.process_received_fs_message(msg, &mut orbit_fs);
                    }
                    Err(_) => {
                        // Deserialization failed - rollback the buffer and stop trying
                        current_buffer = buffer_before;
                        break;
                    }
                }
            }

            // Convert remaining Bytes back to BytesMut for next iteration
            buffer = BytesMut::from(current_buffer);
        }
        Ok(())
    }

    async fn forward_fs_messages(
        &self,
        mut net_sender: SendStream,
        mut receiver: Receiver<FsMessages>,
    ) -> Result<(), anyhow::Error> {
        while let Ok(msg) = receiver.recv().await {
            debug!("Sending FS message {:?}", msg);
            let bytes = serialize_fs_message(&msg);
            net_sender.write_all(&bytes).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl NetworkCommunication for IrohNetworkCommunication {
    fn notify_latest_block(&self, history_data: HistoryData, block: Block) {
        if let Err(e) = self
            .fs_message_sender
            .send(FsMessages::NotifyHistory(history_data))
        {
            return warn!("Failed to send network message: {}", e);
        }
        if let Err(e) = self
            .fs_message_sender
            .send(FsMessages::NotifyLatestBlock(block))
        {
            warn!("Failed to send network message: {}", e);
        }
    }

    fn request_file_with_callback(
        &self,
        content_hash: ContentHash,
        timeout: Duration,
        callback: Box<dyn FnOnce(bool) + Send>,
    ) {
        if let Err(e) = self
            .fs_message_sender
            .send(FsMessages::ContentRequest(vec![content_hash.clone()]))
        {
            warn!("Failed to send network message: {}", e);
        }
        let mut content_notifier = self.content_notification_sender.subscribe();
        self.tokio_runtime_handle.spawn(async move {
            let timeout_future = tokio::time::sleep(timeout);
            tokio::pin!(timeout_future);
            loop {
                tokio::select! {
                    _ = &mut timeout_future => {
                        warn!("Timed out waiting for file {}", content_hash);
                        tokio::task::spawn_blocking(move || callback(false));
                        return;
                    }
                    Ok(received_content_hash) = content_notifier.recv() => {
                        if received_content_hash == content_hash {
                            tokio::task::spawn_blocking(move || callback(true));
                            return;
                        }
                    }
                }
            }
        });
    }
}

// Message serialization functions

pub fn serialize_fs_message(m: &FsMessages) -> Bytes {
    let mut writer = BytesMut::new().writer();
    ciborium::into_writer(m, &mut writer).expect("To be able to serialize the FS message");
    writer.into_inner().freeze()
}

pub fn serialize_ctrl_message(m: &CtrlMessages) -> Bytes {
    let mut writer = BytesMut::new().writer();
    ciborium::into_writer(m, &mut writer).expect("To be able to serialize the control message");
    writer.into_inner().freeze()
}
