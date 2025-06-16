use std::{env, thread};
use std::num::ParseIntError;

use bytes::{BufMut, BytesMut};
use iroh::endpoint::Connection;
use iroh::PublicKey;
use iroh::{discovery::mdns::MdnsDiscovery, NodeAddr, SecretKey, Endpoint};
use pfs::network::{Messages, APLN};
use pfs::{Pfs, FsNodeHash};

use tokio::sync::mpsc::{Receiver, Sender};

use log::{info, LevelFilter};

pub fn decode_hex(s: &str) -> Result<Vec<u8>, ParseIntError> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect()
}

fn initialize_pfs(net_sender: Sender<(FsNodeHash, FsNodeHash)>) -> Pfs {
    let data_home = env::var("XDG_DATA_HOME").or(env::var("HOME").map(|h| h + "/.local/share")).expect("Either XDG_DATA_HOME or HOME must be set");
    let data_dir = data_home + "/pfs_data";
    std::fs::create_dir_all(&data_dir).expect("To create the data dir");
    std::fs::create_dir_all(data_dir.clone() + "/tmp").expect("To create the temporary file dir");
    Pfs::initialize(data_dir, Some(net_sender)).expect("Failed to initialize filesystem")
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    env_logger::Builder::from_default_env()
        .filter(None, LevelFilter::Warn)
        .filter(Some("pfs"), LevelFilter::Debug)
        .init();
    let args: Vec<String> = env::args().collect();
    let remote_node_id = decode_hex(&args[1])?;
    let mut remote_pub_key = [0u8; 32];
    remote_pub_key.copy_from_slice(&remote_node_id);
    let mut rng = rand::rngs::OsRng;
    let secret_key = SecretKey::generate(&mut rng);
    let endpoint = Endpoint::builder().discovery(Box::new(MdnsDiscovery::new(secret_key.public())?)).secret_key(secret_key).alpns(vec![APLN.as_bytes().to_vec()]).bind().await?;
    let node_addr = NodeAddr::new(PublicKey::from_bytes(&remote_pub_key)?);
    let (sender, receiver) = tokio::sync::mpsc::channel(10);
    let conn = endpoint.connect(node_addr, APLN.as_bytes()).await?;

    let pfs = initialize_pfs(sender);
    {
        let pfs = pfs.clone();
        thread::spawn(move || {
            run_fs(pfs);
        });
    }
    let _ = forward_root_hash(conn, receiver, pfs).await;
    Ok(())
}

async fn forward_root_hash(conn: Connection, mut recv: Receiver<(FsNodeHash, FsNodeHash)>, _pfs: Pfs) -> Result<(), anyhow::Error> {
    let (mut net_sender, _net_receiver) = conn.open_bi().await?;
    while let Some((_old_hash, new_hash)) = recv.recv().await {
        let msg = Messages::RootHashChanged(new_hash.0);
        let mut datagram = BytesMut::new().writer();
        ciborium::into_writer(&msg, &mut datagram).expect("To be able to serialize the message");
        let _ = net_sender.write_chunk(datagram.into_inner().freeze()).await;
    };
    Ok(())
}



fn run_fs(fs: Pfs) {
    let Ok(home) = env::var("HOME") else {
        println!("$HOME must be set");
        return;
    };
    let mount_point = home + "/Orbit";
    std::fs::create_dir_all(&mount_point).expect("To create the mount point");
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
