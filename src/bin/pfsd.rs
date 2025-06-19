use std::num::ParseIntError;
use std::{env, fs, path::PathBuf, thread};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use clap::Parser;
use iroh::PublicKey;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, NodeAddr, SecretKey, discovery::mdns::MdnsDiscovery};
use pfs::network::{APLN, Messages};
use pfs::{FsNodeHash, InodeNumber, Pfs};
use serde::{Deserialize, Serialize};

use tokio::sync::broadcast::{Receiver, Sender};

use log::{LevelFilter, debug, error, info, warn};

pub fn decode_hex(s: &str) -> Result<Vec<u8>, ParseIntError> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect()
}

#[derive(Parser)]
#[command(name = "pfsd")]
#[command(about = "PFS daemon for distributed content-addressed filesystem")]
struct Args {
    /// Path to configuration file
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Remote node public key to connect to (hex format). If not provided, runs without network connection.
    remote_node: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Config {
    /// Private key for this node (hex encoded)
    private_key: Option<String>,
    /// Directory to store filesystem data
    data_dir: String,
    /// Mount point for the filesystem
    mount_point: String,
}

impl Default for Config {
    fn default() -> Self {
        let home = env::var("HOME").expect("HOME environment variable must be set");
        let data_home =
            env::var("XDG_DATA_HOME").unwrap_or_else(|_| format!("{}/.local/share", home));

        Self {
            private_key: None,
            data_dir: format!("{}/pfs_data", data_home),
            mount_point: format!("{}/Orbit", home),
        }
    }
}

impl Config {
    fn load_from_file(path: &PathBuf) -> Result<Self, anyhow::Error> {
        let content = fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    fn save_to_file(&self, path: &PathBuf) -> Result<(), anyhow::Error> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let content = toml::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }

    fn get_default_config_path() -> PathBuf {
        let config_home = env::var("XDG_CONFIG_HOME").unwrap_or_else(|_| {
            let home = env::var("HOME").expect("HOME environment variable must be set");
            format!("{}/.config", home)
        });
        PathBuf::from(config_home).join("pfsd.toml")
    }

    fn load_or_create(config_path: Option<PathBuf>) -> Result<Self, anyhow::Error> {
        let path = config_path.unwrap_or_else(Self::get_default_config_path);

        let mut config = if path.exists() {
            debug!("Loading config from: {:?}", path);
            Self::load_from_file(&path)?
        } else {
            debug!("Creating new config at: {:?}", path);
            Self::default()
        };

        // Generate private key if not present
        if config.private_key.is_none() {
            let mut rng = rand::rngs::OsRng;
            let secret_key = SecretKey::generate(&mut rng);
            config.private_key = Some(hex::encode(secret_key.to_bytes()));
            debug!("Generated new private key");
        }

        // Save the config (with any generated defaults)
        config.save_to_file(&path)?;
        debug!("Saved config to: {:?}", path);

        Ok(config)
    }

    fn get_secret_key(&self) -> Result<SecretKey, anyhow::Error> {
        let key_hex = self
            .private_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Private key not set in config"))?;
        let key_bytes = hex::decode(key_hex)?;
        if key_bytes.len() != 32 {
            return Err(anyhow::anyhow!("Private key must be 32 bytes"));
        }
        let mut key_array = [0u8; 32];
        key_array.copy_from_slice(&key_bytes);
        let secret_key = SecretKey::from_bytes(&key_array);
        Ok(secret_key)
    }
}

fn initialize_pfs(config: &Config, net_sender: Option<Sender<(FsNodeHash, FsNodeHash)>>) -> Pfs {
    let data_dir = &config.data_dir;
    std::fs::create_dir_all(data_dir).expect("To create the data dir");
    std::fs::create_dir_all(format!("{}/tmp", data_dir)).expect("To create the temporary file dir");
    Pfs::initialize(data_dir.clone(), net_sender).expect("Failed to initialize filesystem")
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    env_logger::Builder::from_default_env()
        .filter(None, LevelFilter::Warn)
        .filter(Some("pfs"), LevelFilter::Debug)
        .init();

    let args = Args::parse();
    let config = Config::load_or_create(args.config)?;

    debug!("Using config: {:?}", config);
    info!("Data directory: {}", config.data_dir);
    info!("Mount point: {}", config.mount_point);

    let secret_key = config.get_secret_key()?;
    info!(
        "Node public key: {}",
        hex::encode(secret_key.public().as_bytes())
    );
    let endpoint = Endpoint::builder()
        .discovery(Box::new(MdnsDiscovery::new(secret_key.public())?))
        .secret_key(secret_key)
        .alpns(vec![APLN.as_bytes().to_vec()])
        .bind()
        .await?;

    let (sender, receiver) = tokio::sync::broadcast::channel(10);
    let pfs = initialize_pfs(&config, Some(sender.clone()));
    {
        let pfs = pfs.clone();
        let mount_point = config.mount_point.clone();
        thread::spawn(move || {
            run_fs(pfs, mount_point);
        });
    }
    {
        let pfs = pfs.clone();
        let endpoint = endpoint.clone();
        tokio::spawn(async move { accept_connections(endpoint, pfs, sender) });
    }

    // Check if we should connect to a remote node
    if let Some(remote_node_str) = args.remote_node {
        info!("Connecting to remote node: {}", remote_node_str);

        let remote_node_id = decode_hex(&remote_node_str)?;
        let mut remote_pub_key = [0u8; 32];
        remote_pub_key.copy_from_slice(&remote_node_id);

        let node_addr = NodeAddr::new(PublicKey::from_bytes(&remote_pub_key)?);
        if let Ok(conn) = endpoint.connect(node_addr, APLN.as_bytes()).await {
            if let Err(e) = handle_connection(conn, pfs, receiver).await {
                warn!("Error while handling new connection: {}", e);
            };
        }
    } else {
        info!("Running in standalone mode (no remote node)");
    }
    // Keep the main task running indefinitely in standalone mode
    tokio::signal::ctrl_c().await?;
    info!("Received Ctrl+C, shutting down...");

    Ok(())
}

async fn accept_connections(
    endpoint: Endpoint,
    pfs: Pfs,
    sender: Sender<(FsNodeHash, FsNodeHash)>,
) -> Result<(), anyhow::Error> {
    while let Some(incommig) = endpoint.accept().await {
        if let Ok(conn) = incommig.await {
            let pfs = pfs.clone();
            let receiver = sender.subscribe();
            tokio::spawn(async move { handle_connection(conn, pfs, receiver).await });
        } else {
            info!("Failed to accept incomming connection");
        }
    }
    Ok(())
}

async fn handle_connection(
    conn: Connection,
    pfs: Pfs,
    receiver: Receiver<(FsNodeHash, FsNodeHash)>,
) -> Result<(), anyhow::Error> {
    let (net_sender, net_receiver) = conn.open_bi().await?;

    {
        let pfs = pfs.clone();
        tokio::spawn(async move { listen_for_updates(net_receiver, pfs) });
    }
    tokio::spawn(async move { forward_root_hash(net_sender, receiver, pfs).await });
    Ok(())
}

fn serialize_message(m: &Messages) -> Bytes {
    let mut writer = BytesMut::new().writer();
    ciborium::into_writer(m, &mut writer).expect("To be able to serialize the message");
    writer.into_inner().freeze()
}

async fn listen_for_updates(
    mut net_receiver: RecvStream,
    mut pfs: Pfs,
) -> Result<(), anyhow::Error> {
    while let Some(chunk) = net_receiver.read_chunk(15_000, true).await? {
        let msg: Messages = ciborium::from_reader(chunk.bytes.reader())?;
        match msg {
            Messages::NewDirectories(dirs) => {
                for dir in dirs {
                    let _ = pfs.persist_directory(&dir);
                }
            }
            Messages::NewFsNodes(nodes) => {
                for node in nodes {
                    let _ = pfs.persist_fs_node(&node);
                }
            }
            Messages::RootHashChanged(new_root_hash) => {
                let new_root_hash = FsNodeHash(new_root_hash);
                let old_root_hash = pfs.get_root_node().calculate_hash();
                let _ =
                    pfs.update_directory_recursive(&old_root_hash, &new_root_hash, InodeNumber(1));
            }
        }
    }
    Ok(())
}

async fn forward_root_hash(
    mut net_sender: SendStream,
    mut recv: Receiver<(FsNodeHash, FsNodeHash)>,
    pfs: Pfs,
) -> Result<(), anyhow::Error> {
    while let Ok((old_hash, new_hash)) = recv.recv().await {
        let old_node = pfs.load_fs_node(&old_hash).expect("to find node");
        let new_node = pfs.load_fs_node(&new_hash).expect("to find node");
        let (updated_fs_nodes, updated_directories) = pfs.diff(&old_node, &new_node);
        let _ = net_sender
            .write_chunk(serialize_message(&Messages::NewFsNodes(updated_fs_nodes)))
            .await;
        let _ = net_sender
            .write_chunk(serialize_message(&Messages::NewDirectories(
                updated_directories,
            )))
            .await;
        let _ = net_sender
            .write_chunk(serialize_message(&Messages::RootHashChanged(new_hash.0)))
            .await;
    }
    Ok(())
}

fn run_fs(fs: Pfs, mount_point: String) {
    if let Err(e) = std::fs::create_dir_all(&mount_point) {
        error!("Failed to ensure mount point exists: {}", e);
        return;
    };
    info!("Mounting to {}", mount_point);
    let (send, recv) = std::sync::mpsc::channel();
    let send_ctrlc = send.clone();
    ctrlc::set_handler(move || {
        send_ctrlc.send(()).unwrap();
    })
    .unwrap();
    let guard = fuser::spawn_mount2(fs, &mount_point, &vec![]).unwrap();
    let () = recv.recv().unwrap();
    drop(guard)
}
