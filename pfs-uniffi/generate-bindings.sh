#!/bin/bash
# Generate UniFFI bindings for PFS

set -e

LANGUAGE=${1:-kotlin}
BUILD_TYPE=${2:-release}

# Build the library
echo "Building pfs-uniffi library..."
cargo build --${BUILD_TYPE} -p pfs-uniffi

# Determine library extension based on OS
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    LIB_EXT="so"
elif [[ "$OSTYPE" == "darwin"* ]]; then
    LIB_EXT="dylib"
else
    LIB_EXT="dll"
fi

LIB_PATH="../target/${BUILD_TYPE}/libpfs_uniffi.${LIB_EXT}"
OUT_DIR="bindings/${LANGUAGE}"

echo "Generating ${LANGUAGE} bindings..."
mkdir -p "$OUT_DIR"

# Generate bindings using uniffi-bindgen
cargo run --bin uniffi-bindgen generate \
    --library "$LIB_PATH" \
    --language "$LANGUAGE" \
    --out-dir "$OUT_DIR"

echo "✓ Bindings generated successfully in ${OUT_DIR}/"
echo ""
echo "Next steps:"
case $LANGUAGE in
    kotlin)
        echo "  1. Copy the generated .kt files to your Kotlin project"
        echo "  2. Copy the library file to your project's native library directory"
        echo "  3. Import with: import uniffi.pfs_uniffi.*"
        ;;
    swift)
        echo "  1. Add the generated .swift file to your Xcode project"
        echo "  2. Add the library file to your project's frameworks"
        ;;
    python)
        echo "  1. Copy the generated .py file and library to your Python path"
        echo "  2. Import with: import pfs_uniffi"
        ;;
esac
