use fuser::BackgroundSession;
use pfs::Pfs;
use std::fs;
use std::io::Write;
use std::thread;
use std::time::Duration;

fn setup() -> (BackgroundSession, String) {
    let uuid = uuid::Uuid::new_v4();
    let mount_point = format!("/tmp/{}/pfs", uuid);
    let data_dir = format!("/tmp/{}/pfs_data", uuid);
    std::fs::create_dir_all(&mount_point).expect("To create the mount point");
    std::fs::create_dir_all(&data_dir).expect("To create the data dir");
    let fs = Pfs::empty(data_dir);
    let guard = fuser::spawn_mount2(fs, &mount_point, &vec![]).unwrap();
    (guard, mount_point)
}

#[test]
fn create_hello_world_file() {
    let (guard, mount_point) = setup();
    
    // Wait for filesystem to be ready
    thread::sleep(Duration::from_millis(100));
    
    let uuid = uuid::Uuid::new_v4();
    let file_path = format!("{}/{}", mount_point, uuid);
    
    // Create and write to file
    {
        let mut file = fs::File::create_new(&file_path).expect("To create file");
        file.write_all(b"Hello World!").expect("To write to file");
        file.flush().expect("To flush file");
    } // File handle is dropped here
    
    // Wait for file operations to complete
    thread::sleep(Duration::from_millis(100));
    
    // Read file content
    let content = fs::read_to_string(&file_path).expect("To read file");
    assert_eq!(content, "Hello World!");
    
    drop(guard);
}

#[test]
fn create_and_read_multiple_files() {
    let (guard, mount_point) = setup();
    thread::sleep(Duration::from_millis(100));
    
    let files = vec![
        ("file1.txt", "Content of file 1"),
        ("file2.txt", "Different content for file 2"),
        ("file3.txt", "Yet another file with different content"),
    ];
    
    // Create files
    for (filename, content) in &files {
        let file_path = format!("{}/{}", mount_point, filename);
        {
            let mut file = fs::File::create_new(&file_path).expect("To create file");
            file.write_all(content.as_bytes()).expect("To write to file");
            file.flush().expect("To flush file");
        }
        thread::sleep(Duration::from_millis(50));
    }
    
    // Read and verify files
    for (filename, expected_content) in &files {
        let file_path = format!("{}/{}", mount_point, filename);
        let actual_content = fs::read_to_string(&file_path).expect("To read file");
        assert_eq!(actual_content, *expected_content);
    }
    
    drop(guard);
}

#[test]
fn create_directory() {
    let (guard, mount_point) = setup();
    thread::sleep(Duration::from_millis(100));
    
    let dir_path = format!("{}/test_dir", mount_point);
    fs::create_dir(&dir_path).expect("To create directory");
    
    thread::sleep(Duration::from_millis(100));
    
    // Verify directory exists
    let metadata = fs::metadata(&dir_path).expect("To get directory metadata");
    assert!(metadata.is_dir());
    
    drop(guard);
}

#[test]
fn create_nested_directories() {
    let (guard, mount_point) = setup();
    thread::sleep(Duration::from_millis(100));
    
    // Create first level directory
    let dir1_path = format!("{}/dir1", mount_point);
    fs::create_dir(&dir1_path).expect("To create first directory");
    thread::sleep(Duration::from_millis(50));
    
    // Create second level directory
    let dir2_path = format!("{}/dir1/dir2", mount_point);
    fs::create_dir(&dir2_path).expect("To create second directory");
    thread::sleep(Duration::from_millis(50));
    
    // Verify both directories exist
    assert!(fs::metadata(&dir1_path).unwrap().is_dir());
    assert!(fs::metadata(&dir2_path).unwrap().is_dir());
    
    drop(guard);
}

#[test]
fn create_file_in_directory() {
    let (guard, mount_point) = setup();
    thread::sleep(Duration::from_millis(100));
    
    // Create directory
    let dir_path = format!("{}/test_dir", mount_point);
    fs::create_dir(&dir_path).expect("To create directory");
    thread::sleep(Duration::from_millis(50));
    
    // Create file in directory
    let file_path = format!("{}/test_file.txt", dir_path);
    {
        let mut file = fs::File::create_new(&file_path).expect("To create file in directory");
        file.write_all(b"File in directory").expect("To write to file");
        file.flush().expect("To flush file");
    }
    thread::sleep(Duration::from_millis(50));
    
    // Verify file content
    let content = fs::read_to_string(&file_path).expect("To read file");
    assert_eq!(content, "File in directory");
    
    drop(guard);
}

#[test]
fn list_directory_contents() {
    let (guard, mount_point) = setup();
    thread::sleep(Duration::from_millis(100));
    
    // Create some files and directories
    let items = vec![
        ("file1.txt", false),
        ("file2.txt", false),
        ("subdir", true),
    ];
    
    for (name, is_dir) in &items {
        let path = format!("{}/{}", mount_point, name);
        if *is_dir {
            fs::create_dir(&path).expect("To create directory");
        } else {
            let mut file = fs::File::create_new(&path).expect("To create file");
            file.write_all(b"test content").expect("To write to file");
            file.flush().expect("To flush file");
        }
        thread::sleep(Duration::from_millis(50));
    }
    
    // List directory contents
    let entries: Vec<_> = fs::read_dir(&mount_point)
        .expect("To read directory")
        .map(|entry| entry.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    
    // Verify all items are present
    for (name, _) in &items {
        assert!(entries.contains(&name.to_string()), "Missing: {}", name);
    }
    
    drop(guard);
}

#[test]
fn write_and_read_empty_file() {
    let (guard, mount_point) = setup();
    thread::sleep(Duration::from_millis(100));
    
    let file_path = format!("{}/empty_file.txt", mount_point);
    
    // Create empty file
    {
        let file = fs::File::create_new(&file_path).expect("To create empty file");
        drop(file);
    }
    thread::sleep(Duration::from_millis(50));
    
    // Read empty file
    let content = fs::read_to_string(&file_path).expect("To read empty file");
    assert_eq!(content, "");
    
    drop(guard);
}

#[test]
fn write_large_file() {
    let (guard, mount_point) = setup();
    thread::sleep(Duration::from_millis(100));
    
    let file_path = format!("{}/large_file.txt", mount_point);
    let large_content = "x".repeat(10000); // 10KB file
    
    // Create large file
    {
        let mut file = fs::File::create_new(&file_path).expect("To create large file");
        file.write_all(large_content.as_bytes()).expect("To write large content");
        file.flush().expect("To flush file");
    }
    thread::sleep(Duration::from_millis(200));
    
    // Read and verify large file
    let content = fs::read_to_string(&file_path).expect("To read large file");
    assert_eq!(content, large_content);
    assert_eq!(content.len(), 10000);
    
    drop(guard);
}
