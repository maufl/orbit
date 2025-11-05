use clap::Parser;
use pfs::Pfs;
use pfs::config::Config;
use pfs::network::{
    NetworkCommunication, IrohNetworkCommunication,
};
use std::sync::Arc;
use std::{path::PathBuf, thread};
use tokio::sync::broadcast::Receiver;

use log::{LevelFilter, debug, error, info};

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
    info!(
        "Node public key: {}",
        hex::encode(secret_key.public().as_bytes())
    );
    let (shutdown_sender, shutdown_receiver) = tokio::sync::broadcast::channel(1);
    let iroh_network_communication = Arc::new(IrohNetworkCommunication::build(secret_key).await?);
    let pfs = initialize_pfs(&config, iroh_network_communication.clone());
    info!(
        "Current root hash is {}",
        pfs.get_root_node().calculate_hash()
    );
    {
        let pfs = pfs.clone();
        let mount_point = config.mount_point.clone();
        thread::spawn(move || run_fs(pfs, mount_point, shutdown_receiver));
    }


    // Connect to all peer node IDs from config
    let peer_node_ids = config.peer_node_ids.clone();
    iroh_network_communication.connect_to_all_peers(peer_node_ids, pfs).await;
    // Keep the main task running indefinitely in standalone mode
    tokio::signal::ctrl_c().await?;
    let _ = shutdown_sender.send(());
    info!("Received Ctrl+C, shutting down...");

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
