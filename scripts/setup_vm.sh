#!/usr/bin/env bash
# One-time setup of a fresh Ubuntu 24.04 cPouta VM: OS updates, Docker Engine + Compose, swap.
# Docker install follows https://docs.docker.com/engine/install/ubuntu/ (official apt repository).
# Usage: bash scripts/setup_vm.sh   (as the default "ubuntu" user; uses sudo)
set -euo pipefail

echo "== OS updates"
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get -y upgrade
sudo DEBIAN_FRONTEND=noninteractive apt-get -y install ca-certificates curl git openssl

if ! command -v docker >/dev/null; then
  echo "== Docker Engine"
  sudo install -m 0755 -d /etc/apt/keyrings
  sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  sudo chmod a+r /etc/apt/keyrings/docker.asc
  # shellcheck disable=SC1091  # /etc/os-release exists on the VM
  sudo tee /etc/apt/sources.list.d/docker.sources > /dev/null <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF
  sudo apt-get update
  sudo DEBIAN_FRONTEND=noninteractive apt-get -y install \
    docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi
sudo usermod -aG docker "$USER"

# standard.large has 7.8 GiB RAM; a swap file protects against out-of-memory kills during builds.
if ! swapon --show | grep -q .; then
  echo "== 4 GiB swap file"
  sudo fallocate -l 4G /swapfile
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile
  sudo swapon /swapfile
  echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab > /dev/null
fi

docker_version=$(sudo docker --version)
compose_version=$(sudo docker compose version)
echo "== Done: ${docker_version}; ${compose_version}"
echo "Log out and back in (exit, then ssh cpouta) so you can run docker without sudo."
