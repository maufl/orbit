use std::io::Write;
use std::time::Duration;

use crate::{Block, BlockHash, ContentHash, Directory, FsNode, FsNodeHash, InodeNumber, OrbitFs};
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
    Hello(Block),
    BlockChanged(Block),
    BlockRequest(BlockHash),
    BlockResponse(Block),
    BlockRangeRequest((BlockHash, BlockHash)), // (start_inclusive, end_exclusive)
    BlockRangeResponse(Vec<Block>),
    FsNodeRequest(Vec<FsNodeHash>),
    FsNodeResponse(Vec<FsNode>),
    NewFsNodes(Vec<FsNode>),
    NewDirectories(Vec<Directory>),
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
    /// Request a range of blocks from the network (start_inclusive to end_exclusive)
    /// Returns the blocks when they become available or None on timeout
    async fn request_blocks(
        &self,
        start_block: BlockHash,
        end_block: BlockHash,
        timeout: Duration,
    ) -> Option<Vec<Block>>;
    /// Request FsNodes by their hashes from the network
    /// Returns the nodes when they become available or None on timeout
    async fn request_fsnodes(
        &self,
        fs_node_hashes: Vec<FsNodeHash>,
        timeout: Duration,
    ) -> Option<Vec<FsNode>>;
}

/// Implementation of network communication using tokio broadcast channels
pub struct IrohNetworkCommunication {
    message_sender: Sender<Messages>,
    endpoint: Endpoint,
    #[allow(dead_code)] // Reserved for future use
    tokio_runtime_handle: Handle,
    content_notification_sender: Sender<ContentHash>,
    block_notification_sender: Sender<Vec<Block>>,
    fsnode_notification_sender: Sender<Vec<FsNode>>,
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
        let (fsnode_notification_sender, _fsnode_notification_receiver) =
            tokio::sync::broadcast::channel(10);
        let (sender, _receiver) = tokio::sync::broadcast::channel(10);
        Ok(IrohNetworkCommunication {
            message_sender: sender.clone(),
            endpoint,
            tokio_runtime_handle: tokio::runtime::Handle::current(),
            content_notification_sender,
            block_notification_sender,
            fsnode_notification_sender,
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
                .connect_to_peer_impl(peer_node_id.clone(), orbit_fs.clone())
                .await
            {
                warn!("Error connecting to peer {}: {}", peer_node_id, e)
            }
        }
    }

    pub fn accept_connections(&self, orbit_fs: OrbitFs) {
        let endpoint = self.endpoint.clone();
        let message_sender = self.message_sender.clone();
        let content_notification_sender = self.content_notification_sender.clone();
        let block_notification_sender = self.block_notification_sender.clone();
        let fsnode_notification_sender = self.fsnode_notification_sender.clone();

        tokio::spawn(async move {
            if let Err(e) = Self::accept_connections_impl(
                endpoint,
                orbit_fs,
                message_sender,
                content_notification_sender,
                block_notification_sender,
                fsnode_notification_sender,
            )
            .await
            {
                warn!("Error accepting connections: {}", e);
            }
        });
    }

    async fn connect_to_peer_impl(
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

        let endpoint = self.endpoint.clone();
        let message_sender = self.message_sender.clone();
        let content_notification_sender = self.content_notification_sender.clone();
        let block_notification_sender = self.block_notification_sender.clone();
        let fsnode_notification_sender = self.fsnode_notification_sender.clone();

        tokio::spawn(async move {
            let conn = loop {
                let node_addr = node_addr.clone();
                match endpoint.connect(node_addr, APLN.as_bytes()).await {
                    Ok(conn) => break conn,
                    Err(e) => {
                        warn!("Failed to connect to node {}: {}", peer_node_id, e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    }
                }
            };
            if let Err(e) = Self::open_connection(
                conn,
                orbit_fs,
                message_sender,
                content_notification_sender,
                block_notification_sender,
                fsnode_notification_sender,
            )
            .await
            {
                warn!("Error while handling connection to {}: {}", peer_node_id, e);
            }
        });

        Ok(())
    }

    async fn accept_connections_impl(
        endpoint: Endpoint,
        orbit_fs: OrbitFs,
        sender: Sender<Messages>,
        content_notifier: Sender<ContentHash>,
        block_notifier: Sender<Vec<Block>>,
        fsnode_notifier: Sender<Vec<FsNode>>,
    ) -> Result<(), anyhow::Error> {
        while let Some(incommig) = endpoint.accept().await {
            if let Ok(conn) = incommig.await {
                info!("Accepted new connection from {}", conn.remote_id());
                let orbit_fs_clone = orbit_fs.clone();
                let (net_sender, net_receiver) = conn.accept_bi().await?;
                let content_notifier = content_notifier.clone();
                let block_notifier = block_notifier.clone();
                let fsnode_notifier = fsnode_notifier.clone();
                let message_sender = sender.clone();
                Self::handle_connection(
                    net_sender,
                    net_receiver,
                    orbit_fs_clone,
                    content_notifier,
                    block_notifier,
                    fsnode_notifier,
                    message_sender,
                )
                .await?;
            } else {
                warn!("Failed to accept incomming connection");
            }
        }
        Ok(())
    }

    async fn open_connection(
        conn: Connection,
        orbit_fs: OrbitFs,
        message_sender: Sender<Messages>,
        content_notifier: Sender<ContentHash>,
        block_notifier: Sender<Vec<Block>>,
        fsnode_notifier: Sender<Vec<FsNode>>,
    ) -> Result<(), anyhow::Error> {
        info!("Handling new connection to {}", conn.remote_id());
        let (net_sender, net_receiver) = conn.open_bi().await?;
        Self::handle_connection(
            net_sender,
            net_receiver,
            orbit_fs,
            content_notifier,
            block_notifier,
            fsnode_notifier,
            message_sender,
        )
        .await
    }

    async fn handle_connection(
        net_sender: SendStream,
        net_receiver: RecvStream,
        orbit_fs: OrbitFs,
        content_notifier: Sender<ContentHash>,
        block_notifier: Sender<Vec<Block>>,
        fsnode_notifier: Sender<Vec<FsNode>>,
        message_sender: Sender<Messages>,
    ) -> Result<(), anyhow::Error> {
        let receiver = message_sender.subscribe();
        let current_block = orbit_fs.runtime_data.read().current_block.clone();
        if let Err(e) = message_sender.send(Messages::Hello(current_block)) {
            warn!("Unable to queue hello message for new connection: {}", e);
        };
        tokio::spawn(async move {
            Self::listen_for_updates(
                net_receiver,
                message_sender.clone(),
                orbit_fs,
                content_notifier,
                block_notifier,
                fsnode_notifier,
            )
            .await
            .inspect_err(|e| warn!("Stopped listening for updates, encountered error {}", e))
        });
        tokio::spawn(async move {
            Self::forward_messages(net_sender, receiver)
                .await
                .inspect_err(|e| warn!("Stopped sending updated, encountered error{}", e))
        });
        Ok(())
    }

    fn process_received_message(
        msg: Messages,
        orbit_fs: &mut OrbitFs,
        message_sender: &Sender<Messages>,
        content_notifier: &Sender<ContentHash>,
        block_notifier: &Sender<Vec<Block>>,
        fsnode_notifier: &Sender<Vec<FsNode>>,
    ) {
        match msg {
            Messages::NewDirectories(dirs) => {
                debug!("Received a NewDirectories message");
                for dir in dirs {
                    if let Err(e) = orbit_fs.persistence.persist_directory(&dir) {
                        warn!("Unable to persist directories: {}", e);
                    }
                }
            }
            Messages::NewFsNodes(nodes) => {
                debug!("Received a NewFsNodes message");
                for node in nodes {
                    if let Err(e) = orbit_fs.persistence.persist_fs_node(&node) {
                        warn!("Unable to persist FS node: {}", e);
                    }
                }
            }
            Messages::BlockChanged(new_block) => {
                debug!("Received a BlockChanged message");

                // Get the current block and calculate hashes
                let current_block = orbit_fs.runtime_data.read().current_block.clone();
                let current_block_hash = current_block.calculate_hash();
                let new_block_hash = new_block.calculate_hash();

                // Persist the new block first so ancestry checks can load it
                if let Err(e) = orbit_fs.persistence.persist_block(&new_block) {
                    warn!("Unable to persist new block: {}", e);
                    return;
                }

                // Check if the new block is an ancestor of our current block
                // If so, we already have a newer version, so we can ignore this update
                if orbit_fs.is_block_ancestor(&new_block_hash, &current_block_hash) {
                    debug!(
                        "Received block {} is an ancestor of our current block {}, ignoring",
                        new_block_hash, current_block_hash
                    );
                    return;
                }

                // Check if our current block is an ancestor of the new block
                // If so, we can safely update to the new block
                if !orbit_fs.is_block_ancestor(&current_block_hash, &new_block_hash) {
                    warn!(
                        "Received block {} is not a descendant of our current block {}, this indicates a divergence",
                        new_block_hash, current_block_hash
                    );

                    // Try to find the common ancestor
                    if let Some(common_ancestor) =
                        orbit_fs.find_common_ancestor(&current_block_hash, &new_block_hash)
                    {
                        info!(
                            "Found common ancestor block {} for diverged branches {} and {}",
                            common_ancestor, current_block_hash, new_block_hash
                        );
                        // TODO: Implement three-way merge using the common ancestor
                        // For now, we'll skip the update to avoid potential data loss
                    } else {
                        warn!(
                            "Could not find common ancestor between {} and {}, blocks may be from different filesystems",
                            current_block_hash, new_block_hash
                        );
                    }
                    return;
                }

                // Our current block is an ancestor of the new block, proceed with update
                info!(
                    "Updating from block {} to block {}",
                    current_block_hash, new_block_hash
                );

                let new_root_hash = new_block.root_node_hash;
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
            Messages::Hello(block) => {
                let block_hash = block.calculate_hash();
                debug!(
                    "Received Hello message from peer, their block hash is {}",
                    block_hash
                );

                let current_block = orbit_fs.runtime_data.read().current_block.clone();
                let current_block_hash = current_block.calculate_hash();

                // Persist the block if we don't have it
                if let Err(_) = orbit_fs.persistence.load_block(&block_hash) {
                    info!(
                        "The remote's block {} is unknown to us, persisting it",
                        block_hash
                    );
                    if let Err(e) = orbit_fs.persistence.persist_block(&block) {
                        warn!("Unable to persist block from hello: {}", e);
                        return;
                    }

                    // Check if the remote block is ahead of us
                    if orbit_fs.is_block_ancestor(&current_block_hash, &block_hash) {
                        info!(
                            "Remote block {} is ahead of our current block {}, requesting intermediate blocks",
                            block_hash, current_block_hash
                        );
                        // Request all blocks between our current block and the remote block
                        if let Err(e) = message_sender.send(Messages::BlockRangeRequest((
                            block_hash,
                            current_block_hash,
                        ))) {
                            warn!("Failed to send block range request: {}", e);
                        }
                    } else {
                        debug!(
                            "Remote block {} is not ahead of us, no need to request intermediate blocks",
                            block_hash
                        );
                    }
                } else {
                    debug!("We know this remote block!");
                }
            }
            Messages::BlockRequest(block_hash) => {
                debug!("Received BlockRequest for {}", block_hash);
                match orbit_fs.persistence.load_block(&block_hash) {
                    Ok(block) => {
                        if let Err(e) = message_sender.send(Messages::BlockResponse(block)) {
                            warn!("Failed to send block response: {}", e);
                        }
                    }
                    Err(e) => {
                        warn!("Unable to load requested block {}: {}", block_hash, e);
                    }
                }
            }
            Messages::BlockResponse(block) => {
                debug!("Received BlockResponse");
                if let Err(e) = orbit_fs.persistence.persist_block(&block) {
                    warn!("Unable to persist block from response: {}", e);
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
                    if let Err(e) = message_sender.send(Messages::ContentResponse((
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
                    if let Err(e) = content_notifier.send(content_hash) {
                        warn!("Failed to notify about new content: {}", e)
                    }
                };
            }
            Messages::BlockRangeRequest((start_block, end_block)) => {
                debug!(
                    "Received BlockRangeRequest from {} to {}",
                    start_block, end_block
                );
                let mut blocks = Vec::new();
                let mut current_hash = start_block;

                // Walk backwards from start_block until we reach end_block or hit the beginning
                loop {
                    // If we've reached the end_block (exclusive), stop
                    if current_hash == end_block {
                        break;
                    }

                    // Try to load the current block
                    match orbit_fs.persistence.load_block(&current_hash) {
                        Ok(block) => {
                            let prev_hash = block.previous_blocks.0;
                            blocks.push(block);

                            // If we've reached the beginning, stop
                            if prev_hash == BlockHash::default() {
                                break;
                            }

                            current_hash = prev_hash;
                        }
                        Err(e) => {
                            warn!(
                                "Unable to load block {} in range request: {}",
                                current_hash, e
                            );
                            break;
                        }
                    }
                }

                debug!("Sending BlockRangeResponse with {} blocks", blocks.len());
                if let Err(e) = message_sender.send(Messages::BlockRangeResponse(blocks)) {
                    warn!("Failed to send block range response: {}", e);
                }
            }
            Messages::BlockRangeResponse(blocks) => {
                debug!("Received BlockRangeResponse with {} blocks", blocks.len());

                // Persist all received blocks
                for block in &blocks {
                    if let Err(e) = orbit_fs.persistence.persist_block(block) {
                        warn!("Unable to persist block from range response: {}", e);
                    }
                }

                // Notify that blocks are available
                if let Err(e) = block_notifier.send(blocks) {
                    warn!("Failed to notify about received blocks: {}", e);
                }
            }
            Messages::FsNodeRequest(fs_node_hashes) => {
                debug!("Received FsNodeRequest for {} nodes", fs_node_hashes.len());
                let mut nodes = Vec::new();

                for hash in fs_node_hashes {
                    match orbit_fs.persistence.load_fs_node(&hash) {
                        Ok(node) => nodes.push(node),
                        Err(e) => {
                            warn!("Unable to load requested FsNode {}: {}", hash, e);
                        }
                    }
                }

                debug!("Sending FsNodeResponse with {} nodes", nodes.len());
                if let Err(e) = message_sender.send(Messages::FsNodeResponse(nodes)) {
                    warn!("Failed to send FsNode response: {}", e);
                }
            }
            Messages::FsNodeResponse(nodes) => {
                debug!("Received FsNodeResponse with {} nodes", nodes.len());

                // Persist all received nodes
                for node in &nodes {
                    if let Err(e) = orbit_fs.persistence.persist_fs_node(node) {
                        warn!("Unable to persist FsNode from response: {}", e);
                    }
                }

                // Notify that nodes are available
                if let Err(e) = fsnode_notifier.send(nodes) {
                    warn!("Failed to notify about received FsNodes: {}", e);
                }
            }
        }
    }

    async fn listen_for_updates(
        mut net_receiver: RecvStream,
        message_sender: Sender<Messages>,
        mut orbit_fs: OrbitFs,
        content_notifier: Sender<ContentHash>,
        block_notifier: Sender<Vec<Block>>,
        fsnode_notifier: Sender<Vec<FsNode>>,
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
                        Self::process_received_message(
                            msg,
                            &mut orbit_fs,
                            &message_sender,
                            &content_notifier,
                            &block_notifier,
                            &fsnode_notifier,
                        );
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

    async fn request_blocks(
        &self,
        start_block: BlockHash,
        end_block: BlockHash,
        timeout: Duration,
    ) -> Option<Vec<Block>> {
        self.send_message(Messages::BlockRangeRequest((start_block, end_block)));
        let mut block_notifier = self.block_notification_sender.subscribe();

        let timeout_future = tokio::time::sleep(timeout);
        tokio::pin!(timeout_future);

        tokio::select! {
            _ = &mut timeout_future => {
                warn!("Timed out waiting for blocks from {} to {}", start_block, end_block);
                None
            }
            Ok(blocks) = block_notifier.recv() => {
                Some(blocks)
            }
        }
    }

    async fn request_fsnodes(
        &self,
        fs_node_hashes: Vec<FsNodeHash>,
        timeout: Duration,
    ) -> Option<Vec<FsNode>> {
        self.send_message(Messages::FsNodeRequest(fs_node_hashes.clone()));
        let mut fsnode_notifier = self.fsnode_notification_sender.subscribe();

        let timeout_future = tokio::time::sleep(timeout);
        tokio::pin!(timeout_future);

        tokio::select! {
            _ = &mut timeout_future => {
                warn!("Timed out waiting for {} FsNodes", fs_node_hashes.len());
                None
            }
            Ok(fsnodes) = fsnode_notifier.recv() => {
                Some(fsnodes)
            }
        }
    }
}

// Connection management functions

// Message handling functions

pub fn serialize_message(m: &Messages) -> Bytes {
    let mut writer = BytesMut::new().writer();
    ciborium::into_writer(m, &mut writer).expect("To be able to serialize the message");
    writer.into_inner().freeze()
}
