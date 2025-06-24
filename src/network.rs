use std::time::Duration;

use crate::{ContentHash, Directory, FsNode};
use bytes::Bytes;
use log::warn;
use serde::{Deserialize, Serialize};
use tokio::{
    runtime::Handle,
    sync::broadcast::{Receiver, Sender},
};

pub const APLN: &str = "de.maufl.pfs";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Messages {
    Hello([u8; 32]),
    RootHashChanged([u8; 32]),
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
