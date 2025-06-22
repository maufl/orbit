use bytes::{Buf, BufMut, Bytes, BytesMut};
use clap::Parser;
use iroh::PublicKey;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, NodeAddr, discovery::mdns::MdnsDiscovery};
use pfs::config::Config;
use pfs::network::{APLN, Messages};
use pfs::{FsNodeHash, InodeNumber, Pfs};
use std::time::Duration;
use std::{path::PathBuf, thread};

use tokio::sync::broadcast::{Receiver, Sender};

use log::{LevelFilter, debug, error, info, warn};

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
    info!("Node ID is {}", endpoint.node_id());
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
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            if let Err(e) = accept_connections(endpoint, pfs, sender).await {
                warn!("Error accepting connections: {}", e);
            }
        });
    }

    // Check if we should connect to a remote node
    if let Some(remote_node_str) = args.remote_node {
        info!("Connecting to remote node: {}", remote_node_str);

        let remote_node_id = hex::decode(&remote_node_str)?;
        let mut remote_pub_key = [0u8; 32];
        remote_pub_key.copy_from_slice(&remote_node_id);

        let node_addr = NodeAddr::new(PublicKey::from_bytes(&remote_pub_key)?);
        match endpoint.connect(node_addr, APLN.as_bytes()).await {
            Ok(conn) => {
                if let Err(e) = open_connection(conn, pfs, receiver).await {
                    warn!("Error while handling new connection: {}", e);
                }
            }
            Err(e) => warn!("Unable to connect to {:?}, {}", remote_pub_key, e),
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
        debug!("Incomming connection from {}", incommig.remote_address());
        if let Ok(conn) = incommig.await {
            debug!("Acceping connection from {:?}", conn.remote_node_id());
            let pfs = pfs.clone();
            let receiver = sender.subscribe();
            let (net_sender, net_receiver) = conn.accept_bi().await?;
            {
                let pfs = pfs.clone();
                tokio::spawn(async move { listen_for_updates(net_receiver, pfs).await });
            }
            tokio::spawn(async move { forward_root_hash(net_sender, receiver, pfs).await });
        } else {
            info!("Failed to accept incomming connection");
        }
    }
    Ok(())
}

async fn open_connection(
    conn: Connection,
    pfs: Pfs,
    receiver: Receiver<(FsNodeHash, FsNodeHash)>,
) -> Result<(), anyhow::Error> {
    debug!("Handling new connection from {}", conn.remote_node_id()?);
    let (net_sender, net_receiver) = conn.open_bi().await?;

    {
        let pfs = pfs.clone();
        tokio::spawn(async move {
            listen_for_updates(net_receiver, pfs)
                .await
                .inspect_err(|e| warn!("Stopped listening for updates, encountered error {}", e))
        });
    }
    tokio::spawn(async move {
        forward_root_hash(net_sender, receiver, pfs)
            .await
            .inspect_err(|e| warn!("Stopped sending updated, encountered error{}", e))
    });
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
        info!("Received update from peer");
        let mut reader = chunk.bytes.reader();
        while let Ok(msg) = ciborium::from_reader(&mut reader) {
            debug!("Message is {:?}", msg);
            match msg {
                Messages::NewDirectories(dirs) => {
                    for dir in dirs {
                        if let Err(e) = pfs.persist_directory(&dir) {
                            warn!("Unable to persist directories: {}", e);
                        }
                    }
                }
                Messages::NewFsNodes(nodes) => {
                    for node in nodes {
                        if let Err(e) = pfs.persist_fs_node(&node) {
                            warn!("Unable to persist FS node: {}", e);
                        }
                    }
                }
                Messages::RootHashChanged(new_root_hash) => {
                    let new_root_hash = FsNodeHash(new_root_hash);
                    let old_root_hash = pfs.get_root_node().calculate_hash();
                    if let Err(e) = pfs.update_directory_recursive(
                        &old_root_hash,
                        &new_root_hash,
                        InodeNumber(1),
                    ) {
                        error!("Unable to update root hash: {}", e);
                    }
                }
                Messages::Hello => debug!("Received Hello message from peer"),
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
    debug!("Sending hello");
    let _ = net_sender
        .write_chunk(serialize_message(&Messages::Hello))
        .await;
    while let Ok((old_hash, new_hash)) = recv.recv().await {
        info!("Root hash changes, sending updates to peer");
        let old_node = pfs.load_fs_node(&old_hash).expect("to find node");
        let new_node = pfs.load_fs_node(&new_hash).expect("to find node");
        let (updated_fs_nodes, updated_directories) = pfs.diff(&old_node, &new_node);
        if let Err(e) = net_sender
            .write_chunk(serialize_message(&Messages::NewFsNodes(updated_fs_nodes)))
            .await
        {
            warn!("Error sending new fs nodes: {}", e);
        }
        if let Err(e) = net_sender
            .write_chunk(serialize_message(&Messages::NewDirectories(
                updated_directories,
            )))
            .await
        {
            warn!("Error sending new directories: {}", e);
        }
        if let Err(e) = net_sender
            .write_chunk(serialize_message(&Messages::RootHashChanged(new_hash.0)))
            .await
        {
            warn!("Error sending changed root hash: {}", e)
        }
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
