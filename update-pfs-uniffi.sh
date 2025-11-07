#!/bin/bash
ARCH=${1:-x86_64}
ANDROID_ARCH=$([ "$ARCH" == "x86_64" ] && echo "x86_64" || echo "arm64-v8a")
TARGET=$ARCH-linux-android
cargo ndk build -p pfs-uniffi --target=$TARGET
cp target/$TARGET/debug/libpfs_uniffi.so android/app/src/main/jniLibs/$ANDROID_ARCH/
cargo run -p uniffi-bindgen generate --library target/$TARGET/debug/libpfs_uniffi.so --language kotlin --out-dir android/app/src/main/java/ --no-format