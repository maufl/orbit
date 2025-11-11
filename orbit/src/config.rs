use std::{env, fs, path::PathBuf};

use anyhow::Error;
use iroh::SecretKey;
use log::debug;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Config {
    /// Private key for this node (hex encoded)
    pub private_key: Option<String>,
    /// Directory to store filesystem data
    pub data_dir: String,
    /// Mount point for the filesystem
    pub mount_point: String,
    /// List of peer node IDs to connect to on startup (hex encoded)
    pub peer_node_ids: Vec<String>,
}

pub fn new_secret_key() -> SecretKey {
    SecretKey::generate(&mut rand::rng())
}

impl Default for Config {
    fn default() -> Self {
        let home = env::var("HOME").expect("HOME environment variable must be set");
        let data_home =
            env::var("XDG_DATA_HOME").unwrap_or_else(|_| format!("{}/.local/share", home));

        Self {
            private_key: None,
            data_dir: format!("{}/orbit_data", data_home),
            mount_point: format!("{}/Orbit", home),
            peer_node_ids: Vec::new(),
        }
    }
}

impl Config {
    pub fn load_from_file(path: &PathBuf) -> Result<Self, Error> {
        let content = fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn save_to_file(&self, path: &PathBuf) -> Result<(), Error> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let content = toml::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }

    pub fn get_default_config_path() -> PathBuf {
        let config_home = env::var("XDG_CONFIG_HOME").unwrap_or_else(|_| {
            let home = env::var("HOME").expect("HOME environment variable must be set");
            format!("{}/.config", home)
        });
        PathBuf::from(config_home).join("orbitd.toml")
    }

    pub fn load_or_create(path: PathBuf) -> Result<Self, Error> {
        let mut config = if path.exists() {
            debug!("Loading config from: {:?}", path);
            Self::load_from_file(&path)?
        } else {
            debug!("Creating new config at: {:?}", path);
            Self::default()
        };

        // Generate private key if not present
        if config.private_key.is_none() {
            config.private_key = Some(hex::encode(new_secret_key().to_bytes()));
            debug!("Generated new private key");
        }

        // Save the config (with any generated defaults)
        config.save_to_file(&path)?;
        debug!("Saved config to: {:?}", path);

        Ok(config)
    }

    pub fn get_secret_key(&self) -> Result<SecretKey, Error> {
        let key_hex = self
            .private_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Private key not set in config"))?;
        decode_secret_key(key_hex)
    }

    pub fn add_peer_node_id(&mut self, node_id: String) {
        if !self.peer_node_ids.contains(&node_id) {
            self.peer_node_ids.push(node_id);
        }
    }
}

pub fn encode_secret_key(secret_key: &SecretKey) -> String {
    hex::encode(secret_key.to_bytes())
}

pub fn decode_secret_key(key_hex: &String) -> Result<SecretKey, Error> {
    let key_bytes = hex::decode(key_hex)?;
    if key_bytes.len() != 32 {
        return Err(anyhow::anyhow!("Private key must be 32 bytes"));
    }
    let mut key_array = [0u8; 32];
    key_array.copy_from_slice(&key_bytes);
    let secret_key = SecretKey::from_bytes(&key_array);
    Ok(secret_key)
}
