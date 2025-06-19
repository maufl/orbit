use std::num::ParseIntError;
use std::{path::PathBuf, thread};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use clap::Parser;
use iroh::PublicKey;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, NodeAddr, discovery::mdns::MdnsDiscovery};
use pfs::config::Config;
use pfs::network::{APLN, Messages};
use pfs::{FsNodeHash, InodeNumber, Pfs};

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
    let (shutdown_sender, shutdown_receiver) = tokio::sync::broadcast::channel(1);
    let (sender, receiver) = tokio::sync::broadcast::channel(10);
    let pfs = initialize_pfs(&config, Some(sender.clone()));
    {
        let pfs = pfs.clone();
        let mount_point = config.mount_point.clone();
        thread::spawn(move || run_fs(pfs, mount_point, shutdown_receiver));
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
    let _ = shutdown_sender.send(());
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

fn run_fs(fs: Pfs, mount_point: String, mut close_receiver: Receiver<()>) {
    if let Err(e) = std::fs::create_dir_all(&mount_point) {
        error!("Failed to ensure mount point exists: {}", e);
        return;
    };
    info!("Mounting to {}", mount_point);
    let guard = fuser::spawn_mount2(fs, &mount_point, &vec![]).unwrap();
    let _ = close_receiver.blocking_recv();
    drop(guard)
}
