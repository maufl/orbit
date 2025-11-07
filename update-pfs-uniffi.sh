#!/bin/bash
cargo ndk build -p pfs-uniffi --target=x86_64-linux-android
cp target/x86_64-linux-android/debug/libpfs_uniffi.so android/app/src/main/jniLibs/x86_64
cargo run -p uniffi-bindgen generate --library target/x86_64-linux-android/debug/libpfs_uniffi.so --language kotlin --out-dir android/app/src/main/java/ --no-format