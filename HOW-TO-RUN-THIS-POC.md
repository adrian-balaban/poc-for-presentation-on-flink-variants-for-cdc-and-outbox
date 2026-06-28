# Installation Guide

## Target OS

**Ubuntu 24.04 LTS** (or 22.04+), minimum **20 GB RAM** (container images alone use ~8 GB; k8s path needs more).

---

## 0. One-shot install script (all prerequisites)

If you'd rather not step through sections 1–5 manually, run the standalone script below. It is **idempotent** (safe to re-run) and targets **Ubuntu 24.04/22.04**. It installs the Podman-path prerequisites by default; pass `--with-k8s` to also install the Kubernetes toolchain (Docker, kind, kubectl, Helm, Terraform) used only by `./gradlew allK8s`.

It also installs **Podman Desktop** via Flatpak — a GUI for containers / pods / compose, useful for inspecting the POC stack visually. The CLI `podman` / `podman-compose` it relies on are installed first.

```bash
./scripts/install-prereqs.sh           # Podman path only
./scripts/install-prereqs.sh --with-k8s # Podman + k8s toolchain
```

> The script installs everything with `sudo` to system paths; Podman Desktop is installed per-user under `~/.local/share/flatpak`. After it finishes, continue to [section 6](#6-verify-everything) to confirm the toolchain, then run the POC via [Quick Start](#quick-start-after-installation).

Sections 1–5 below document the same steps in copy-pasteable form for users who want to install prereqs by hand. The script is the source of truth — it is generated from these steps and is the recommended path.

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
