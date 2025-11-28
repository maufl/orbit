use anyhow::Result;
use dialoguer::{Select, theme::ColorfulTheme};
use orbit::config::Config;
use orbit::rpc::OrbitRpcClient;
use tokio::net::UnixStream;

#[tokio::main]
async fn main() -> Result<()> {
    // Get socket path from config
    let config = Config::load_or_create(Config::get_default_config_path())?;
    let socket_path = config.socket_path;

    // Connect to orbitd via Unix socket
    let stream = match UnixStream::connect(&socket_path).await {
        Ok(stream) => stream,
        Err(e) => {
            eprintln!("Failed to connect to orbitd at {}", socket_path);
            eprintln!("Please make sure orbitd is running.");
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    // Create RPC client transport
    let codec_builder = tarpc::tokio_serde::formats::Bincode::default();
    let transport = tarpc::serde_transport::new(
        tokio_util::codec::Framed::new(stream, tokio_util::codec::LengthDelimitedCodec::new()),
        codec_builder,
    );

    let client = OrbitRpcClient::new(tarpc::client::Config::default(), transport).spawn();

    // Main menu loop
    loop {
        let actions = vec!["Connect to a new peer", "Exit"];

        let selection = Select::with_theme(&ColorfulTheme::default())
            .with_prompt("What would you like to do?")
            .items(&actions)
            .default(0)
            .interact()?;

        match selection {
            0 => {
                // Connect to a new peer
                if let Err(e) = connect_to_peer(&client).await {
                    eprintln!("Error: {}", e);
                }
            }
            1 => {
                // Exit
                println!("Goodbye!");
                break;
            }
            _ => unreachable!(),
        }
    }

    Ok(())
}

async fn connect_to_peer(client: &OrbitRpcClient) -> Result<()> {
    // Get discovered peers from orbitd
    let peers = client
        .get_discovered_peers(tarpc::context::current())
        .await?;

    if peers.is_empty() {
        println!("No peers discovered on the local network.");
        println!("Make sure other Orbit nodes are running and discoverable.");
        return Ok(());
    }

    // Create menu items with peer names and node IDs
    let menu_items: Vec<String> = peers
        .iter()
        .map(|peer| format!("{} ({})", peer.name, &peer.node_id[..16]))
        .collect();

    // Show peer selection menu
    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("Select a peer to connect to")
        .items(&menu_items)
        .default(0)
        .interact_opt()?;

    if let Some(idx) = selection {
        let selected_peer = &peers[idx];
        println!(
            "Adding peer: {} ({})",
            selected_peer.name, selected_peer.node_id
        );

        // Call add_peer RPC
        match client
            .add_peer(tarpc::context::current(), selected_peer.node_id.clone())
            .await?
        {
            Ok(()) => {
                println!("✓ Successfully added peer!");
                println!("  - Peer added to configuration");
                println!("  - Control message sent to peer");
                println!("  - FS connection established");
            }
            Err(err) => {
                eprintln!("✗ Failed to add peer: {}", err);
            }
        }
    } else {
        println!("No peer selected.");
    }

    Ok(())
}
