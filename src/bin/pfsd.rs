use base64::Engine;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use clap::Parser;
use iroh::PublicKey;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, NodeAddr, discovery::mdns::MdnsDiscovery};
use pfs::config::Config;
use pfs::network::{APLN, Messages, TokioNetworkCommunication};
use pfs::{FsNodeHash, InodeNumber, Pfs};
use std::time::Duration;
use std::{path::PathBuf, thread};
use tokio::runtime::Handle;

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

fn initialize_pfs(config: &Config, handle: Handle, net_sender: Option<Sender<Messages>>) -> Pfs {
    let data_dir = &config.data_dir;
    std::fs::create_dir_all(data_dir).expect("To create the data dir");
    std::fs::create_dir_all(format!("{}/tmp", data_dir)).expect("To create the temporary file dir");
    
    let network_communication = net_sender.map(|sender| {
        std::sync::Arc::new(TokioNetworkCommunication::new(sender, handle)) as std::sync::Arc<dyn pfs::network::NetworkCommunication>
    });
    
    Pfs::initialize(data_dir.clone(), network_communication)
        .expect("Failed to initialize filesystem")
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
    let pfs = initialize_pfs(
        &config,
        tokio::runtime::Handle::current(),
        Some(sender.clone()),
    );
    info!(
        "Current root hash is {}",
        pfs.get_root_node().calculate_hash()
    );
    {
        let pfs = pfs.clone();
        let mount_point = config.mount_point.clone();
        thread::spawn(move || run_fs(pfs, mount_point, shutdown_receiver));
    }
    {
        let pfs = pfs.clone();
        let endpoint = endpoint.clone();
        let sender = sender.clone();
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
                if let Err(e) = open_connection(conn, pfs, receiver, sender).await {
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
    sender: Sender<Messages>,
) -> Result<(), anyhow::Error> {
    while let Some(incommig) = endpoint.accept().await {
        if let Ok(conn) = incommig.await {
            let pfs = pfs.clone();
            let receiver = sender.subscribe();
            let (net_sender, net_receiver) = conn.accept_bi().await?;
            {
                let pfs = pfs.clone();
                let sender = sender.clone();
                tokio::spawn(async move { listen_for_updates(net_receiver, sender, pfs).await });
            }
            tokio::spawn(async move { forward_messages(net_sender, receiver).await });
        } else {
            info!("Failed to accept incomming connection");
        }
    }
    Ok(())
}

async fn open_connection(
    conn: Connection,
    pfs: Pfs,
    receiver: Receiver<Messages>,
    message_sender: Sender<Messages>,
) -> Result<(), anyhow::Error> {
    debug!("Handling new connection from {}", conn.remote_node_id()?);
    let (net_sender, net_receiver) = conn.open_bi().await?;

    {
        let pfs = pfs.clone();
        tokio::spawn(async move {
            listen_for_updates(net_receiver, message_sender, pfs)
                .await
                .inspect_err(|e| warn!("Stopped listening for updates, encountered error {}", e))
        });
    }
    tokio::spawn(async move {
        forward_messages(net_sender, receiver)
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
    message_sender: Sender<Messages>,
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
                        if let Err(e) = pfs.persistence.persist_directory(&dir) {
                            warn!("Unable to persist directories: {}", e);
                        }
                    }
                }
                Messages::NewFsNodes(nodes) => {
                    for node in nodes {
                        if let Err(e) = pfs.persistence.persist_fs_node(&node) {
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
                    } else {
                        info!("New root hash is {}", pfs.get_root_node().calculate_hash());
                    }
                }
                Messages::Hello(root_hash) => {
                    let root_hash = FsNodeHash(root_hash);
                    debug!(
                        "Received Hello message from peer, their root hash is {}",
                        root_hash
                    );
                    if let Err(err) = pfs.persistence.load_fs_node(&root_hash) {
                        info!(
                            "The remotes root node {} is unknown to us: {}",
                            root_hash, err
                        );
                    } else {
                        debug!("We know this remote root node!");
                    };
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
                    };
                }
            }
        }
    }
    Ok(())
}

async fn forward_messages(
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
