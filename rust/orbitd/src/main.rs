use clap::Parser;
use orbit::OrbitFs;
use orbit::config::Config;
use orbit::network::{IrohNetworkCommunication, NetworkCommunication};
use orbit::rpc::OrbitRpc;
use std::sync::Arc;
use std::{path::PathBuf, thread};
use tokio::net::UnixListener;
use tokio::sync::broadcast::Receiver;

use log::{LevelFilter, debug, error, info};

#[derive(Parser)]
#[command(name = "orbitd")]
#[command(about = "Orbit daemon for distributed content-addressed filesystem")]
struct Args {
    /// Path to configuration file
    #[arg(short, long)]
    config: Option<PathBuf>,

    /// Remote node public key to connect to (hex format). If not provided, runs without network connection.
    remote_node: Option<String>,
}

fn initialize_orbit_fs(
    config: &Config,
    network_communication: Arc<dyn NetworkCommunication>,
) -> OrbitFs {
    let data_dir = &config.data_dir;
    std::fs::create_dir_all(data_dir).expect("To create the data dir");
    std::fs::create_dir_all(format!("{}/tmp", data_dir)).expect("To create the temporary file dir");

    OrbitFs::initialize(data_dir.clone(), Some(network_communication))
        .expect("Failed to initialize filesystem")
}

/// RPC server implementation
#[derive(Clone)]
struct OrbitRpcServer {
    node_id: String,
    network_communication: Arc<IrohNetworkCommunication>,
}

impl OrbitRpcServer {
    fn new(node_id: String, network_communication: Arc<IrohNetworkCommunication>) -> Self {
        Self {
            node_id,
            network_communication,
        }
    }
}

impl OrbitRpc for OrbitRpcServer {
    async fn get_node_id(self, _context: tarpc::context::Context) -> String {
        self.node_id.clone()
    }

    async fn get_discovered_peers(
        self,
        _context: tarpc::context::Context,
    ) -> Vec<orbit::rpc::DiscoveredPeer> {
        self.network_communication
            .get_discovered_peers()
            .await
            .into_iter()
            .map(|(node_id, name)| orbit::rpc::DiscoveredPeer { node_id, name })
            .collect()
    }
}

async fn run_rpc_server(
    socket_path: String,
    node_id: String,
    network_communication: Arc<IrohNetworkCommunication>,
) -> Result<(), anyhow::Error> {
    // Remove the socket file if it already exists
    if std::path::Path::new(&socket_path).exists() {
        std::fs::remove_file(&socket_path)?;
    }

    // Create parent directory if it doesn't exist
    if let Some(parent) = std::path::Path::new(&socket_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let listener = UnixListener::bind(&socket_path)?;
    info!("RPC server listening on {}", socket_path);

    let server = OrbitRpcServer::new(node_id, network_communication);

    loop {
        let (stream, _) = listener.accept().await?;
        let server = server.clone();

        tokio::spawn(async move {
            use futures::StreamExt;
            use tarpc::server::Channel;

            let codec_builder = tarpc::tokio_serde::formats::Bincode::default();
            let transport = tarpc::serde_transport::new(
                tokio_util::codec::Framed::new(
                    stream,
                    tokio_util::codec::LengthDelimitedCodec::new(),
                ),
                codec_builder,
            );

            let server_impl = server.clone();
            tarpc::server::BaseChannel::with_defaults(transport)
                .execute(server_impl.serve())
                .for_each(|response| async move {
                    tokio::spawn(response);
                })
                .await;
        });
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    env_logger::Builder::from_default_env()
        .filter(None, LevelFilter::Warn)
        .filter(Some("netlink_packet_route"), LevelFilter::Error)
        .filter(Some("iroh::net_report"), LevelFilter::Error)
        .filter(Some("tracing::span"), LevelFilter::Error)
        .filter(Some("orbit"), LevelFilter::Debug)
        .init();

    let args = Args::parse();
    let config_path = args.config.unwrap_or_else(Config::get_default_config_path);
    let mut config = Config::load_or_create(config_path.clone())?;

    // Add remote node ID to config if provided
    if let Some(ref remote_node_str) = args.remote_node {
        config.add_peer_node_id(remote_node_str.clone());
        config.save_to_file(&config_path)?;
        debug!("Added remote node ID to config: {}", remote_node_str);
    }

    debug!("Using config: {:?}", config);
    info!("Data directory: {}", config.data_dir);
    info!("Mount point: {}", config.mount_point);

    let secret_key = config.get_secret_key()?;
    let node_id = hex::encode(secret_key.public().as_bytes());
    info!(
        "Node public key: {} {}",
        node_id,
        base32::encode(base32::Alphabet::Z, secret_key.public().as_bytes())
    );
    let (shutdown_sender, shutdown_receiver) = tokio::sync::broadcast::channel(1);
    let iroh_network_communication =
        Arc::new(IrohNetworkCommunication::build(secret_key, config.node_name.clone()).await?);
    let orbit_fs = initialize_orbit_fs(&config, iroh_network_communication.clone());
    info!(
        "Current root hash is {}",
        orbit_fs.get_root_node().calculate_hash()
    );

    // Start RPC server
    let socket_path = config.socket_path.clone();
    let rpc_node_id = node_id.clone();
    let rpc_network_comm = iroh_network_communication.clone();
    tokio::spawn(async move {
        if let Err(e) = run_rpc_server(socket_path, rpc_node_id, rpc_network_comm).await {
            error!("RPC server error: {}", e);
        }
    });

    {
        let orbit_fs = orbit_fs.clone();
        let mount_point = config.mount_point.clone();
        thread::spawn(move || run_fs(orbit_fs, mount_point, shutdown_receiver));
    }

    // accept incoming connections
    iroh_network_communication.accept_connections(orbit_fs.clone());
    // Connect to all peer node IDs from config
    let peer_node_ids = config.peer_node_ids.clone();
    iroh_network_communication
        .connect_to_all_peers(peer_node_ids, orbit_fs)
        .await;
    // Keep the main task running indefinitely in standalone mode
    tokio::signal::ctrl_c().await?;
    let _ = shutdown_sender.send(());
    info!("Received Ctrl+C, shutting down...");

    Ok(())
}

fn run_fs(fs: OrbitFs, mount_point: String, mut close_receiver: Receiver<()>) {
    if let Err(e) = std::fs::create_dir_all(&mount_point) {
        error!("Failed to ensure mount point exists: {}", e);
        return;
    };
    info!("Mounting to {}", mount_point);
    let guard = fuser::spawn_mount2(fs, &mount_point, &vec![]).unwrap();
    let _ = close_receiver.blocking_recv();
    drop(guard)
}
