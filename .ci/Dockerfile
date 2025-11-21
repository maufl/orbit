FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV CARGO_HOME=/opt/cargo
ENV RUSTUP_HOME=/opt/rustup
ENV ANDROID_HOME=/opt/android-sdk
ENV ANDROID_NDK=/opt/android-sdk/ndk/27.0.12077973
ENV PATH="$CARGO_HOME/bin:$ANDROID_HOME/cmdline-tools/latest/bin:$ANDROID_HOME/platform-tools:$PATH"

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    git \
    pkg-config \
    libfuse3-dev \
    build-essential \
    openjdk-17-jdk-headless \
    python3 \
    python-is-python3 \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && . "$CARGO_HOME/env" \
    && rustup target add aarch64-linux-android x86_64-linux-android \
    && cargo install cargo-ndk

# Install Android SDK
RUN mkdir -p "$ANDROID_HOME/cmdline-tools" \
    && cd "$ANDROID_HOME/cmdline-tools" \
    && curl -o cmdline-tools.zip https://dl.google.com/android/repository/commandlinetools-linux-11076708_latest.zip \
    && unzip -q cmdline-tools.zip \
    && mv cmdline-tools latest \
    && rm cmdline-tools.zip \
    && yes | sdkmanager --licenses || true \
    && sdkmanager "platform-tools" "platforms;android-34" "build-tools;34.0.0" "ndk;27.0.12077973"

WORKDIR /app
