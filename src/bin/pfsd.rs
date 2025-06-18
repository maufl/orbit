use std::{env, thread};
use std::num::ParseIntError;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::PublicKey;
use iroh::{discovery::mdns::MdnsDiscovery, NodeAddr, SecretKey, Endpoint};
use pfs::network::{Messages, APLN};
use pfs::{FsNodeHash, InodeNumber, Pfs};

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
    let (net_sender, _net_receiver) = conn.open_bi().await?;
    let pfs = initialize_pfs(sender);
    {
        let pfs = pfs.clone();
        thread::spawn(move || {
            run_fs(pfs);
        });
    }
    {
        let pfs = pfs.clone();
        tokio::spawn(async move { listen_for_updates(_net_receiver, pfs) });
    }
    let _ = forward_root_hash(net_sender, receiver, pfs).await;
    Ok(())
}

fn serialize_message(m: &Messages) -> Bytes {
    let mut writer = BytesMut::new().writer();
    ciborium::into_writer(m, &mut writer).expect("To be able to serialize the message");
    writer.into_inner().freeze()
}

async fn listen_for_updates(mut net_receiver: RecvStream, mut pfs: Pfs) -> Result<(), anyhow::Error> {
    while let Some(chunk) = net_receiver.read_chunk(15_000, true).await? {
        let msg: Messages = ciborium::from_reader(chunk.bytes.reader())?;
        match msg {
            Messages::NewDirectories(dirs) => {
                for dir in dirs {
                    let _ = pfs.persist_directory(&dir);
                }
            },
            Messages::NewFsNodes(nodes) => {
                for node in nodes {
                    let _ = pfs.persist_fs_node(&node);
                }
            },
            Messages::RootHashChanged(new_root_hash) => {
                let new_root_hash = FsNodeHash(new_root_hash);
                let old_root_hash = pfs.get_root_node().calculate_hash();
                let _ = pfs.update_directory_recursive(&old_root_hash, &new_root_hash, InodeNumber(1));
            }
        }
    };
    Ok(())
}

async fn forward_root_hash(mut net_sender: SendStream, mut recv: Receiver<(FsNodeHash, FsNodeHash)>, pfs: Pfs) -> Result<(), anyhow::Error> {
    while let Some((old_hash, new_hash)) = recv.recv().await {
        let old_node = pfs.load_fs_node(&old_hash).expect("to find node");
        let new_node = pfs.load_fs_node(&new_hash).expect("to find node");
        let (updated_fs_nodes, updated_directories) = pfs.diff(&old_node, &new_node);
        let _ = net_sender.write_chunk(serialize_message(&Messages::NewFsNodes(updated_fs_nodes))).await;
        let _ = net_sender.write_chunk(serialize_message(&Messages::NewDirectories(updated_directories))).await;
        let _ = net_sender.write_chunk(serialize_message(&Messages::RootHashChanged(new_hash.0))).await;
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
