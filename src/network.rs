use crate::{ContentHash, Directory, FsNode};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

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
