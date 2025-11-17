use std::io::Write;
use std::time::Duration;

use crate::persistence::HistoryData;
use crate::{Block, BlockHash, ContentHash, InodeNumber, OrbitFs};
use async_trait::async_trait;
use base64::Engine;
use bytes::{BufMut, Bytes, BytesMut};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, PublicKey, SecretKey};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::{
    runtime::Handle,
    sync::broadcast::{Receiver, Sender},
};

pub const APLN: &str = "de.maufl.orbit";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Messages {
    NotifyLatestBlock(Block),
    HistoryRequest((BlockHash, BlockHash)), // (start_inclusive, end_exclusive)
    NotifyHistory(HistoryData),
    ContentRequest(Vec<ContentHash>),
    ContentResponse((ContentHash, Bytes)),
}

/// Trait for network communication capabilities
#[async_trait]
pub trait NetworkCommunication: Send + Sync {
    /// Send a message over the network
    fn send_message(&self, message: Messages);
    /// Request a file and execute a callback when it becomes available
    fn request_file_with_callback(
        &self,
        content_hash: ContentHash,
        timeout: Duration,
        callback: Box<dyn FnOnce() + Send>,
    );
}

/// Implementation of network communication using tokio broadcast channels
#[derive(Clone)]
pub struct IrohNetworkCommunication {
    message_sender: Sender<Messages>,
    endpoint: Endpoint,
    #[allow(dead_code)] // Reserved for future use
    tokio_runtime_handle: Handle,
    content_notification_sender: Sender<ContentHash>,
    block_notification_sender: Sender<Vec<Block>>,
}

impl IrohNetworkCommunication {
    pub async fn build(secret_key: SecretKey) -> anyhow::Result<IrohNetworkCommunication> {
        let endpoint = Endpoint::builder()
            .secret_key(secret_key)
            .alpns(vec![APLN.as_bytes().to_vec()])
            .bind()
            .await?;
        info!("Node ID is {}", endpoint.id());
        let (content_notification_sender, _content_notification_receiver) =
            tokio::sync::broadcast::channel(10);
        let (block_notification_sender, _block_notification_receiver) =
            tokio::sync::broadcast::channel(10);
        let (sender, _receiver) = tokio::sync::broadcast::channel(10);
        Ok(IrohNetworkCommunication {
            message_sender: sender.clone(),
            endpoint,
            tokio_runtime_handle: tokio::runtime::Handle::current(),
            content_notification_sender,
            block_notification_sender,
        })
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
                match network.endpoint.connect(node_addr, APLN.as_bytes()).await {
                    Ok(conn) => break conn,
                    Err(e) => {
                        warn!("Failed to connect to node {}: {}", peer_node_id, e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    }
                }
            };
            if let Err(e) = network.open_connection(conn, orbit_fs).await {
                warn!("Error while handling connection to {}: {}", peer_node_id, e);
            }
        });

        Ok(())
    }

    async fn accept_connections_impl(&self, orbit_fs: OrbitFs) -> Result<(), anyhow::Error> {
        while let Some(incommig) = self.endpoint.accept().await {
            if let Ok(conn) = incommig.await {
                info!("Accepted new connection from {}", conn.remote_id());
                let orbit_fs_clone = orbit_fs.clone();
                let (net_sender, net_receiver) = conn.accept_bi().await?;
                self.handle_connection(net_sender, net_receiver, orbit_fs_clone)
                    .await?;
            } else {
                warn!("Failed to accept incomming connection");
            }
        }
        Ok(())
    }

    async fn open_connection(
        &self,
        conn: Connection,
        orbit_fs: OrbitFs,
    ) -> Result<(), anyhow::Error> {
        info!("Handling new connection to {}", conn.remote_id());
        let (net_sender, net_receiver) = conn.open_bi().await?;
        self.handle_connection(net_sender, net_receiver, orbit_fs)
            .await
    }

    async fn handle_connection(
        &self,
        net_sender: SendStream,
        net_receiver: RecvStream,
        orbit_fs: OrbitFs,
    ) -> Result<(), anyhow::Error> {
        let receiver = self.message_sender.subscribe();
        let current_block = orbit_fs.runtime_data.read().current_block.clone();
        if let Err(e) = self
            .message_sender
            .send(Messages::NotifyLatestBlock(current_block))
        {
            warn!(
                "Unable to queue latest block notification for new connection: {}",
                e
            );
        };
        let network = self.clone();
        tokio::spawn(async move {
            network
                .listen_for_updates(net_receiver, orbit_fs)
                .await
                .inspect_err(|e| warn!("Stopped listening for updates, encountered error {}", e))
        });
        let network = self.clone();
        tokio::spawn(async move {
            network
                .forward_messages(net_sender, receiver)
                .await
                .inspect_err(|e| warn!("Stopped sending updated, encountered error{}", e))
        });
        Ok(())
    }

    fn process_received_message(&self, msg: Messages, orbit_fs: &mut OrbitFs) {
        match msg {
            Messages::NotifyHistory(history) => {
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
            Messages::NotifyLatestBlock(remote_block) => {
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

                    if let Err(e) = self.message_sender.send(Messages::NotifyHistory(history)) {
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
                if let Err(e) = self.message_sender.send(Messages::HistoryRequest((
                    remote_block_hash,
                    current_block_hash,
                ))) {
                    warn!("Failed to send history request: {}", e);
                }

                let new_root_hash = remote_block.root_node_hash;
                let old_root_hash = orbit_fs.get_root_node().calculate_hash();
                if let Err(e) = orbit_fs.update_directory_recursive(
                    &old_root_hash,
                    &new_root_hash,
                    InodeNumber(1),
                ) {
                    error!("Unable to update root hash: {}", e);
                } else {
                    info!(
                        "Successfully updated to new root hash: {}",
                        orbit_fs.get_root_node().calculate_hash()
                    );
                }
            }
            Messages::ContentRequest(requested_content) => {
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
                    if let Err(e) = self.message_sender.send(Messages::ContentResponse((
                        content_hash,
                        buf.into_inner().freeze(),
                    ))) {
                        warn!("Failed to send content message: {}", e);
                    };
                }
            }
            Messages::ContentResponse((content_hash, bytes)) => {
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
            Messages::HistoryRequest((start_block, end_block)) => {
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
                if let Err(e) = self.message_sender.send(Messages::NotifyHistory(history)) {
                    warn!("Failed to send history notification: {}", e);
                }
            }
        }
    }

    async fn listen_for_updates(
        &self,
        mut net_receiver: RecvStream,
        mut orbit_fs: OrbitFs,
    ) -> Result<(), anyhow::Error> {
        use bytes::Buf;

        let mut buffer = BytesMut::new();
        while let Some(chunk) = net_receiver.read_chunk(15_000, true).await? {
            debug!("Received chunk");
            // Add the new chunk to our buffer
            let mut writer = buffer.writer();
            writer.write_all(&chunk.bytes)?;
            buffer = writer.into_inner();

            // Try to deserialize as many complete messages as possible
            let mut current_buffer = buffer.freeze();
            loop {
                // Save the current buffer position for potential rollback (cheap clone)
                let buffer_before = current_buffer.clone();
                let mut reader = current_buffer.reader();

                match ciborium::from_reader::<Messages, _>(&mut reader) {
                    Ok(msg) => {
                        // Successfully deserialized a message - process it immediately
                        debug!("Message is {:?}", msg);
                        current_buffer = reader.into_inner();

                        // Process the message using the extracted function
                        self.process_received_message(msg, &mut orbit_fs);
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

    async fn forward_messages(
        &self,
        mut net_sender: SendStream,
        mut receiver: Receiver<Messages>,
    ) -> Result<(), anyhow::Error> {
        while let Ok(msg) = receiver.recv().await {
            debug!("Sending message {:?}", msg);
            let bytes = serialize_message(&msg);
            net_sender.write_all(&bytes).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl NetworkCommunication for IrohNetworkCommunication {
    fn send_message(&self, message: Messages) {
        if let Err(e) = self.message_sender.send(message) {
            warn!("Failed to send network message: {}", e);
        }
    }

    fn request_file_with_callback(
        &self,
        content_hash: ContentHash,
        timeout: Duration,
        callback: Box<dyn FnOnce() + Send>,
    ) {
        self.send_message(Messages::ContentRequest(vec![content_hash.clone()]));
        let mut content_notifier = self.content_notification_sender.subscribe();
        self.tokio_runtime_handle.spawn(async move {
            let timeout = tokio::time::sleep(timeout);
            tokio::pin!(timeout);
            loop {
                tokio::select! {
                    _ = &mut timeout => {
                        warn!("Timed out waiting for file {}", content_hash);
                        return;
                    }
                    Ok(received_content_hash) = content_notifier.recv() => {
                        if received_content_hash == content_hash {
                            tokio::task::spawn_blocking(callback);
                            return;
                        }
                    }
                }
            }
        });
    }
}

// Connection management functions

// Message handling functions

pub fn serialize_message(m: &Messages) -> Bytes {
    let mut writer = BytesMut::new().writer();
    ciborium::into_writer(m, &mut writer).expect("To be able to serialize the message");
    writer.into_inner().freeze()
}
