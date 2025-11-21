#!/bin/bash
set -euo pipefail

# CI Build Script for Orbit
# Builds both the Android app and the orbitd binary

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ARTIFACTS_DIR="${PROJECT_ROOT}/artifacts"

echo "=== Orbit CI Build ==="
echo "Project root: $PROJECT_ROOT"

# Create artifacts directory
rm -rf "$ARTIFACTS_DIR"
mkdir -p "$ARTIFACTS_DIR"

# Install Rust targets for Android
echo ""
echo "=== Installing Rust Android targets ==="
rustup target add aarch64-linux-android x86_64-linux-android

# Install cargo-ndk if not present
if ! command -v cargo-ndk &> /dev/null; then
    echo "Installing cargo-ndk..."
    cargo install cargo-ndk
fi

# Build orbitd (Linux binary)
echo ""
echo "=== Building orbitd ==="
cd "$PROJECT_ROOT"
cargo build --release --bin orbitd --features fuse
cp target/release/orbitd "$ARTIFACTS_DIR/"
echo "Built: $ARTIFACTS_DIR/orbitd"

# Build Android app
echo ""
echo "=== Building Android app ==="
cd "$PROJECT_ROOT/android"

# Ensure ANDROID_NDK is set
if [ -z "${ANDROID_NDK:-}" ]; then
    # Try common locations
    if [ -d "$ANDROID_HOME/ndk-bundle" ]; then
        export ANDROID_NDK="$ANDROID_HOME/ndk-bundle"
    elif [ -d "$ANDROID_HOME/ndk" ]; then
        # Find latest NDK version
        ANDROID_NDK=$(find "$ANDROID_HOME/ndk" -maxdepth 1 -type d | sort -V | tail -1)
        export ANDROID_NDK
    else
        echo "Error: ANDROID_NDK not set and could not find NDK"
        exit 1
    fi
fi
echo "Using NDK: $ANDROID_NDK"

./gradlew assembleRelease

# Copy APK to artifacts
cp app/build/outputs/apk/release/*.apk "$ARTIFACTS_DIR/" 2>/dev/null || \
cp app/build/outputs/apk/debug/*.apk "$ARTIFACTS_DIR/" 2>/dev/null || \
echo "Warning: No APK found"

echo ""
echo "=== Build complete ==="
echo "Artifacts:"
ls -lh "$ARTIFACTS_DIR/"
