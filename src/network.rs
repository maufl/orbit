use serde::{Deserialize, Serialize};

pub const APLN: &str = "de.maufl.pfs";

#[derive(Debug, Serialize, Deserialize)]
pub enum Messages {
    RootHashChanged([u8; 32]),
}
