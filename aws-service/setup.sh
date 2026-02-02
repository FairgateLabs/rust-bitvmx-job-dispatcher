#!/bin/bash
set -euo pipefail

## Install dependencies
sudo apt-get update
sudo apt-get install -y git unzip ca-certificates curl gnupg lsb-release build-essential pkg-config libssl-dev

## install Docker
if ! command -v docker >/dev/null 2>&1; then
    sudo mkdir -p /etc/apt/keyrings

    curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

  echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

    sudo apt update
    sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

    sudo usermod -aG docker $USER
    newgrp docker

    if docker info >/dev/null 2>&1; then
        echo "Docker installation successful"
    else
        echo "Docker installation failed" >&2
        exit 1
    fi

    if ! systemctl is-active --quiet docker; then
        sudo systemctl enable docker
        sudo systemctl start docker
    fi

## Install Rust
if ! command -v rustup >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi
if [[ -f "$HOME/.cargo/env" ]]; then
    source "$HOME/.cargo/env"
fi

## Install AWS CLI
if ! command -v aws >/dev/null 2>&1; then
    tmpdir="$(mktemp -d)"
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "$tmpdir/awscliv2.zip"
    unzip -q "$tmpdir/awscliv2.zip" -d "$tmpdir"
    sudo "$tmpdir/aws/install" --update
    rm -rf "$tmpdir"
fi

# Install RiscZero
curl -L https://risczero.com/install | bash
source ~/.bashrc
cargo install cargo-binstall
cargo binstall cargo-risczero --version 3.0.4
rzup install

# Prompt for destination directory
read -r -p "Enter destination directory for clone: " DEST_DIR
if [[ -z "$DEST_DIR" ]]; then
    echo "Destination directory cannot be empty" >&2
    exit 1
fi

mkdir -p "$DEST_DIR"
git clone https://github.com/FairgateLabs/rust-bitvmx-zk-proof.git "$DEST_DIR"

cd "$DEST_DIR/rust-bitvmx-zk-proof"
cargo run --release


