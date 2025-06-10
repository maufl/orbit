use std::env;

use pfs::Pfs;

use log::info;

fn main() {
    simple_logger::init().unwrap();
    let Ok(data_home) = env::var("XDG_DATA_HOME").or(env::var("HOME").map(|h| h + "/.local/share"))
    else {
        println!("Either $XDG_DATA_HOME or $HOME must be set");
        return;
    };
    let mount_point = data_home.clone() + "/pfs";
    let data_dir = data_home + "/pfs_data";
    std::fs::create_dir_all(&data_dir).expect("To create the data dir");
    std::fs::create_dir_all(data_dir.clone() + "/tmp").expect("To create the temporary file dir");
    let fs = Pfs::initialize(data_dir).expect("Failed to initialize filesystem");
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
