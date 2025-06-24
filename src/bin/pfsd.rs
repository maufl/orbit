use base64::Engine;
use bytes::{BufMut, Bytes, BytesMut};
use clap::Parser;
use iroh::PublicKey;
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, NodeAddr, discovery::mdns::MdnsDiscovery};
use pfs::config::Config;
use pfs::network::{APLN, Messages, NetworkCommunication, TokioNetworkCommunication};
use pfs::{ContentHash, FsNodeHash, InodeNumber, Pfs};
use std::io::Write;
use std::sync::Arc;
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

fn initialize_pfs(config: &Config, network_communication: Arc<dyn NetworkCommunication>) -> Pfs {
    let data_dir = &config.data_dir;
    std::fs::create_dir_all(data_dir).expect("To create the data dir");
    std::fs::create_dir_all(format!("{}/tmp", data_dir)).expect("To create the temporary file dir");

    Pfs::initialize(data_dir.clone(), Some(network_communication))
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
    let (content_notification_sender, content_notification_receiver) =
        tokio::sync::broadcast::channel(10);
    let (sender, receiver) = tokio::sync::broadcast::channel(10);
    let network_communication = std::sync::Arc::new(TokioNetworkCommunication::new(
        sender.clone(),
        tokio::runtime::Handle::current(),
        content_notification_receiver,
    )) as std::sync::Arc<dyn pfs::network::NetworkCommunication>;
    let pfs = initialize_pfs(&config, network_communication);
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
        let content_notification_sender = content_notification_sender.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(10)).await;
            if let Err(e) =
                accept_connections(endpoint, pfs, sender, content_notification_sender).await
            {
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
                if let Err(e) =
                    open_connection(conn, pfs, receiver, sender, content_notification_sender).await
                {
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
    content_notifier: Sender<ContentHash>,
) -> Result<(), anyhow::Error> {
    while let Some(incommig) = endpoint.accept().await {
        if let Ok(conn) = incommig.await {
            info!(
                "Accepted new connection from {}",
                conn.remote_node_id().unwrap()
            );
            let pfs = pfs.clone();
            let receiver = sender.subscribe();
            let (net_sender, net_receiver) = conn.accept_bi().await?;
            {
                let pfs = pfs.clone();
                let sender = sender.clone();
                let content_notifier = content_notifier.clone();
                tokio::spawn(async move {
                    listen_for_updates(net_receiver, sender, pfs, content_notifier).await
                });
            }
            tokio::spawn(async move { forward_messages(net_sender, receiver).await });
        } else {
            warn!("Failed to accept incomming connection");
        }
    }
    Ok(())
}

async fn open_connection(
    conn: Connection,
    pfs: Pfs,
    receiver: Receiver<Messages>,
    message_sender: Sender<Messages>,
    content_notifier: Sender<ContentHash>,
) -> Result<(), anyhow::Error> {
    info!("Handling new connection to {}", conn.remote_node_id()?);
    let (net_sender, net_receiver) = conn.open_bi().await?;

    {
        let pfs = pfs.clone();
        let content_notifier = content_notifier.clone();
        tokio::spawn(async move {
            listen_for_updates(net_receiver, message_sender, pfs, content_notifier)
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

fn process_received_message(
    msg: Messages,
    pfs: &mut Pfs,
    message_sender: &Sender<Messages>,
    content_notifier: &Sender<ContentHash>,
) {
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

async fn listen_for_updates(
    mut net_receiver: RecvStream,
    message_sender: Sender<Messages>,
    mut pfs: Pfs,
    content_notifier: Sender<ContentHash>,
) -> Result<(), anyhow::Error> {
    use bytes::Buf;
    
    let mut buffer = BytesMut::new();
    while let Some(chunk) = net_receiver.read_chunk(15_000, true).await? {
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
