# Installation Guide

## Target OS

**Ubuntu 24.04 LTS** (or 22.04+), minimum **20 GB RAM** (container images alone use ~8 GB; k8s path needs more).

---

## 0. One-shot install script (all prerequisites)

If you'd rather not step through sections 1–5 manually, run the script below. It is **idempotent** (safe to re-run) and targets **Ubuntu 24.04/22.04**. It installs the Podman-path prerequisites by default; pass `--with-k8s` to also install the Kubernetes toolchain (Docker, kind, kubectl, Helm, Terraform) used only by `./gradlew allK8s`.

It also installs **Podman Desktop** via Flatpak — a GUI for containers / pods / compose, useful for inspecting the POC stack visually. The CLI `podman` / `podman-compose` it relies on are installed first.

```bash
#!/usr/bin/env bash
# install-prerequisites.sh — Ubuntu 24.04/22.04, idempotent.
# Usage: ./install-prerequisites.sh [--with-k8s]
set -euo pipefail

WITH_K8S=0
[ "${1:-}" = "--with-k8s" ] && WITH_K8S=1

echo "==> apt update / core packages"
sudo apt update
sudo apt install -y \
  openjdk-17-jdk \
  podman podman-compose \
  curl jq bash iproute2 \
  ca-certificates gnupg uidmap flatpak

echo "==> Verify Java 17"
java -version

echo "==> Pin Podman storage (fixes the snap-VS-Code split-storage bug — see CLAUDE.md)"
mkdir -p ~/.config/containers
if ! grep -q '^graphroot' ~/.config/containers/storage.conf 2>/dev/null; then
  cat >> ~/.config/containers/storage.conf <<EOF
[storage]
graphroot = "/home/$USER/.local/share/containers/storage"
EOF
fi

echo "==> Podman Desktop (Flatpak)"
if ! flatpak remote-list 2>/dev/null | grep -q '^flathub'; then
  flatpak remote-add --user --if-not-exists flathub https://flathub.org/repo/flathub.flatpakrepo
fi
flatpak install --user -y flathub io.podman_desktop.PodmanDesktop

if [ "$WITH_K8S" = "1" ]; then
  echo "==> Terraform (HashiCorp APT repo)"
  if ! command -v terraform >/dev/null 2>&1; then
    wget -O- https://apt.releases.hashicorp.com/gpg \
      | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
    echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(. /etc/os-release; echo "$VERSION_CODENAME") main" \
      | sudo tee /etc/apt/sources.list.d/hashicorp.list
    sudo apt update && sudo apt install -y terraform
  fi
  terraform version

  echo "==> Docker (required by kind)"
  if ! command -v docker >/dev/null 2>&1; then
    sudo apt install -y docker.io
    sudo usermod -aG docker "$USER"
  fi
  docker --version

  echo "==> kind"
  if ! command -v kind >/dev/null 2>&1; then
    [ "$(uname -m)" = "aarch64" ] && ARCH=arm64 || ARCH=amd64
    sudo curl -fsSL -o /usr/local/bin/kind "https://kind.sigs.k8s.io/dl/latest/kind-linux-$ARCH"
    sudo chmod +x /usr/local/bin/kind
  fi
  kind version

  echo "==> kubectl"
  if ! command -v kubectl >/dev/null 2>&1; then
    [ "$(uname -m)" = "aarch64" ] && ARCH=arm64 || ARCH=amd64
    sudo curl -fsSL -o /usr/local/bin/kubectl \
      "https://dl.k8s.io/release/$(curl -sL https://dl.k8s.io/release/stable.txt)/bin/linux/$ARCH/kubectl"
    sudo chmod +x /usr/local/bin/kubectl
  fi
  kubectl version --client

  echo "==> Helm 3"
  if ! command -v helm >/dev/null 2>&1; then
    curl -fsSL https://baltocdn.com/helm/signing.asc | sudo gpg --dearmor -o /usr/share/keyrings/helm.gpg
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/helm.gpg] https://baltocdn.com/helm/stable/debian/ all main" \
      | sudo tee /etc/apt/sources.list.d/helm-stable-debian.list
    sudo apt update && sudo apt install -y helm
  fi
  helm version

  echo "==> Python 3 (k8s deploy script)"
  sudo apt install -y python3
  python3 --version
fi

echo "==> Done. Re-login (or 'newgrp docker' if --with-k8s) for group changes to take effect."
echo "==> Then verify with section 6 ('Verify Everything')."
```

> The script installs everything with `sudo` to system paths; Podman Desktop is installed per-user under `~/.local/share/flatpak`. After it finishes, continue to [section 6](#6-verify-everything) to confirm the toolchain, then run the POC via [Quick Start](#quick-start-after-installation).

---

## 1. Core Build Tools

### Java 17 (JDK)

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk

# Verify
java -version   # should show 17.x
```

> Gradle 8.7 is bundled via the wrapper (`./gradlew`) — no separate install needed.

---

## 2. Container Runtime

### Podman + Podman Compose

```bash
sudo apt install -y podman podman-compose
```

> If running VS Code as a snap, pin Podman storage to avoid the split-storage bug
> (see `CLAUDE.md` — "snap VS Code Podman storage split issue"):
>
> ```bash
> mkdir -p ~/.config/containers
> cat >> ~/.config/containers/storage.conf <<'EOF'
> [storage]
> graphroot = "/home/$USER/.local/share/containers/storage"
> EOF
> ```

---

## 3. CLI Utilities

```bash
sudo apt install -y curl jq bash iproute2
```

| Tool | Used by |
|------|---------|
| `curl` | Health checks in `build.gradle`, connector deployment, k8s job cancel |
| `jq` | `deploy-connectors.sh` (parses connector JSON) |
| `bash` | All shell scripts |
| `ss` (iproute2) | `port-forward.sh` (checks if a port is already bound) |

---

## 4. Terraform (optional — Grafana dashboards/alerts)

`./gradlew all` skips Grafana provisioning with a warning if Terraform is absent.

```bash
# HashiCorp APT repository
sudo apt install -y gnupg software-properties-common
wget -O- https://apt.releases.hashicorp.com/gpg | \
  sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
  https://apt.releases.hashicorp.com $(lsb_release -cs) main" | \
  sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt update && sudo apt install -y terraform

# Verify (must be >= 1.6)
terraform -version
```

---

## 5. Kubernetes Tools (only for `./gradlew allK8s`)

Skip this section if you only use the Podman Compose path (`./gradlew all`).

### Docker (required by kind)

kind uses Docker by default to run the Kubernetes node container. `deploy.sh` auto-detects whether Docker or Podman manages the kind node and adjusts `docker exec` / `podman exec` accordingly.

```bash
# Add Docker's official GPG key and repository
sudo apt install -y ca-certificates gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list
sudo apt update && sudo apt install -y docker-ce docker-ce-cli containerd.io

# Allow running Docker without sudo
sudo usermod -aG docker $USER
newgrp docker

# Verify
docker --version
docker run --rm hello-world
```

### kind

```bash
# Download latest kind binary
[ $(uname -m) = x86_64 ] && curl -Lo ./kind https://kind.sigs.k8s.io/dl/latest/kind-linux-amd64
chmod +x ./kind
sudo mv ./kind /usr/local/bin/kind

kind --version
```

### kubectl

```bash
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x kubectl
sudo mv kubectl /usr/local/bin/kubectl

kubectl version --client
```

### Helm 3

```bash
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

helm version
```

### Python 3 (for k8s deploy script)

```bash
sudo apt install -y python3
```

> `deploy.sh` uses `python3` to parse Flink REST JSON when cancelling YAML pipeline jobs before redeployment.

---

## 6. Verify Everything

```bash
java -version          # 17.x
./gradlew --version    # Gradle 8.7 (downloads automatically)
podman --version
podman-compose --version
curl --version
jq --version
terraform -version     # >= 1.6 (optional)

# k8s path only:
docker --version
kind --version
kubectl version --client
helm version
python3 --version
```

---

## Quick Start After Installation

```bash
# Podman path (recommended first run)
cd local-development-podman
podman-compose -f podman-compose.yml up -d --build
cd ..
./gradlew all

# k8s path
./gradlew allK8s
```
