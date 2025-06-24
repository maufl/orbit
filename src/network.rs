use crate::{ContentHash, Directory, FsNode};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::{runtime::Handle, sync::broadcast::Sender};
use log::warn;

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
}

/// Implementation of network communication using tokio broadcast channels
pub struct TokioNetworkCommunication {
    message_sender: Sender<Messages>,
    #[allow(dead_code)] // Reserved for future use
    tokio_runtime_handle: Handle,
}

impl TokioNetworkCommunication {
    pub fn new(message_sender: Sender<Messages>, tokio_runtime_handle: Handle) -> Self {
        Self {
            message_sender,
            tokio_runtime_handle,
        }
    }
}

impl NetworkCommunication for TokioNetworkCommunication {
    fn send_message(&self, message: Messages) {
        if let Err(e) = self.message_sender.send(message) {
            warn!("Failed to send network message: {}", e);
        }
    }
}
