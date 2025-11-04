use std::io::Write;
use std::time::Duration;

use crate::{Block, BlockHash, ContentHash, Directory, FsNode, InodeNumber, Pfs};
use base64::Engine;
use bytes::{BufMut, Bytes, BytesMut};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, NodeAddr, PublicKey};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use tokio::{
    runtime::Handle,
    sync::broadcast::{Receiver, Sender},
};

pub const APLN: &str = "de.maufl.pfs";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Messages {
    Hello(Block),
    BlockChanged(Block),
    BlockRequest(BlockHash),
    BlockResponse(Block),
    NewFsNodes(Vec<FsNode>),
    NewDirectories(Vec<Directory>),
    ContentRequest(Vec<ContentHash>),
    ContentResponse((ContentHash, Bytes)),
}

/// Trait for network communication capabilities
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
pub struct TokioNetworkCommunication {
    message_sender: Sender<Messages>,
    #[allow(dead_code)] // Reserved for future use
    tokio_runtime_handle: Handle,
    content_notifier: Receiver<ContentHash>,
}

impl TokioNetworkCommunication {
    pub fn new(
        message_sender: Sender<Messages>,
        tokio_runtime_handle: Handle,
        content_notifier: Receiver<ContentHash>,
    ) -> Self {
        Self {
            message_sender,
            tokio_runtime_handle,
            content_notifier,
        }
    }
}

impl NetworkCommunication for TokioNetworkCommunication {
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
        let mut content_notifier = self.content_notifier.resubscribe();
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

pub async fn connect_to_peer(
    peer_node_id: String,
    endpoint: Endpoint,
    pfs: Pfs,
    sender: tokio::sync::broadcast::Sender<Messages>,
    content_notification_sender: tokio::sync::broadcast::Sender<ContentHash>,
) -> Result<(), anyhow::Error> {
    info!("Connecting to peer: {}", peer_node_id);

    let remote_node_id = hex::decode(&peer_node_id)
        .map_err(|e| anyhow::anyhow!("Invalid hex format for node ID {}: {}", peer_node_id, e))?;

    let mut remote_pub_key = [0u8; 32];
    remote_pub_key.copy_from_slice(&remote_node_id);

    let node_addr = NodeAddr::new(PublicKey::from_bytes(&remote_pub_key)?);

    tokio::spawn(async move {
        let Ok(conn) = endpoint.connect(node_addr, APLN.as_bytes()).await else {
            return warn!("Failed to connect to node {}", peer_node_id);
        };
        if let Err(e) = open_connection(conn, pfs, sender, content_notification_sender).await {
            warn!("Error while handling connection to {}: {}", peer_node_id, e);
        }
    });

    Ok(())
}

pub async fn connect_to_all_peers(
    peer_node_ids: Vec<String>,
    endpoint: &Endpoint,
    pfs: Pfs,
    sender: tokio::sync::broadcast::Sender<Messages>,
    content_notification_sender: tokio::sync::broadcast::Sender<ContentHash>,
) {
    if peer_node_ids.is_empty() {
        info!("No peer nodes configured, running in standalone mode");
        return;
    }

    info!("Connecting to {} peer(s)", peer_node_ids.len());

    for peer_node_id in peer_node_ids {
        if let Err(e) = connect_to_peer(
            peer_node_id.clone(),
            endpoint.clone(),
            pfs.clone(),
            sender.clone(),
            content_notification_sender.clone(),
        )
        .await
        {
            warn!("Error connecting to peer {}: {}", peer_node_id, e)
        }
    }
}

pub async fn accept_connections(
    endpoint: Endpoint,
    pfs: Pfs,
    sender: Sender<Messages>,
    content_notifier: Sender<ContentHash>,
) -> Result<(), anyhow::Error> {
    while let Some(incommig) = endpoint.accept().await {
        if let Ok(conn) = incommig.await {
            info!(
                "Accepted new connection from {}",
                conn.remote_node_id().unwrap()
            );
            let pfs = pfs.clone();
            let (net_sender, net_receiver) = conn.accept_bi().await?;
            let content_notifier = content_notifier.clone();
            let message_sender = sender.clone();
            handle_connection(
                net_sender,
                net_receiver,
                pfs,
                content_notifier,
                message_sender,
            )
            .await?;
        } else {
            warn!("Failed to accept incomming connection");
        }
    }
    Ok(())
}

pub async fn open_connection(
    conn: Connection,
    pfs: Pfs,
    message_sender: Sender<Messages>,
    content_notifier: Sender<ContentHash>,
) -> Result<(), anyhow::Error> {
    info!("Handling new connection to {}", conn.remote_node_id()?);
    let (net_sender, net_receiver) = conn.open_bi().await?;
    handle_connection(
        net_sender,
        net_receiver,
        pfs,
        content_notifier,
        message_sender,
    )
    .await
}

pub async fn handle_connection(
    net_sender: SendStream,
    net_receiver: RecvStream,
    pfs: Pfs,
    content_notifier: Sender<ContentHash>,
    message_sender: Sender<Messages>,
) -> Result<(), anyhow::Error> {
    let receiver = message_sender.subscribe();
    let current_block = pfs.runtime_data.read().current_block.clone();
    if let Err(e) = message_sender.send(Messages::Hello(current_block)) {
        warn!("Unable to queue hello message for new connection: {}", e);
    };
    tokio::spawn(async move {
        listen_for_updates(net_receiver, message_sender, pfs, content_notifier)
            .await
            .inspect_err(|e| warn!("Stopped listening for updates, encountered error {}", e))
    });
    tokio::spawn(async move {
        forward_messages(net_sender, receiver)
            .await
            .inspect_err(|e| warn!("Stopped sending updated, encountered error{}", e))
    });
    Ok(())
}

// Message handling functions

pub fn serialize_message(m: &Messages) -> Bytes {
    let mut writer = BytesMut::new().writer();
    ciborium::into_writer(m, &mut writer).expect("To be able to serialize the message");
    writer.into_inner().freeze()
}

pub fn process_received_message(
    msg: Messages,
    pfs: &mut Pfs,
    message_sender: &Sender<Messages>,
    content_notifier: &Sender<ContentHash>,
) {
    match msg {
        Messages::NewDirectories(dirs) => {
            debug!("Received a NewDirectories message");
            for dir in dirs {
                if let Err(e) = pfs.persistence.persist_directory(&dir) {
                    warn!("Unable to persist directories: {}", e);
                }
            }
        }
        Messages::NewFsNodes(nodes) => {
            debug!("Received a NewFsNodes message");
            for node in nodes {
                if let Err(e) = pfs.persistence.persist_fs_node(&node) {
                    warn!("Unable to persist FS node: {}", e);
                }
            }
        }
        Messages::BlockChanged(new_block) => {
            debug!("Received a BlockChanged message");
            // First persist the new block
            if let Err(e) = pfs.persistence.persist_block(&new_block) {
                warn!("Unable to persist new block: {}", e);
                return;
            }

            let new_root_hash = new_block.root_node_hash;
            let old_root_hash = pfs.get_root_node().calculate_hash();
            if let Err(e) =
                pfs.update_directory_recursive(&old_root_hash, &new_root_hash, InodeNumber(1))
            {
                error!("Unable to update root hash: {}", e);
            } else {
                info!("New root hash is {}", pfs.get_root_node().calculate_hash());
            }
        }
        Messages::Hello(block) => {
            let block_hash = block.calculate_hash();
            debug!(
                "Received Hello message from peer, their block hash is {}",
                block_hash
            );
            // Persist the block if we don't have it
            if let Err(_) = pfs.persistence.load_block(&block_hash) {
                info!(
                    "The remote's block {} is unknown to us, persisting it",
                    block_hash
                );
                if let Err(e) = pfs.persistence.persist_block(&block) {
                    warn!("Unable to persist block from hello: {}", e);
                }
            } else {
                debug!("We know this remote block!");
            }
        }
        Messages::BlockRequest(block_hash) => {
            debug!("Received BlockRequest for {}", block_hash);
            match pfs.persistence.load_block(&block_hash) {
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
            if let Err(e) = pfs.persistence.persist_block(&block) {
                warn!("Unable to persist block from response: {}", e);
            }
        }
        Messages::ContentRequest(requested_content) => {
            for content_hash in requested_content.into_iter() {
                let file_path = pfs.data_dir.clone()
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
            let new_file_path = pfs.data_dir.clone()
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
    }
}

pub async fn listen_for_updates(
    mut net_receiver: RecvStream,
    message_sender: Sender<Messages>,
    mut pfs: Pfs,
    content_notifier: Sender<ContentHash>,
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
                    process_received_message(msg, &mut pfs, &message_sender, &content_notifier);
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

pub async fn forward_messages(
    mut net_sender: SendStream,
    mut message_receiver: Receiver<Messages>,
) -> Result<(), anyhow::Error> {
    while let Ok(msg) = message_receiver.recv().await {
        if let Err(e) = net_sender.write_chunk(serialize_message(&msg)).await {
            warn!("Failed to send message: {}", e)
        }
    }
    Ok(())
}
