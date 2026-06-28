#!/usr/bin/env bash
# scripts/install-prereqs.sh — Ubuntu 24.04/22.04, idempotent.
#
# Installs everything required to build and run this POC end-to-end:
#   Podman path (default):  Java 17, Podman, podman-compose, curl, jq,
#                          bash, iproute2, ca-certificates, gnupg, uidmap,
#                          flatpak, Podman Desktop (via flatpak)
#   k8s path (--with-k8s):  Docker, kind, kubectl, Helm, Terraform, Python 3
#                          (in addition to the Podman-path defaults)
#
# Usage:
#   ./scripts/install-prereqs.sh              # Podman path only
#   ./scripts/install-prereqs.sh --with-k8s   # Podman + k8s
#
# Idempotent: safe to re-run; skips already-installed components.
# Requires: Ubuntu 24.04 (or 22.04+), sudo privileges, network access.
# After running: re-login (or 'newgrp docker' if --with-k8s) for group
# changes to take effect, then verify with the section
# "6. Verify Everything" in HOW-TO-RUN-THIS-POC.md.

set -euo pipefail

WITH_K8S=0
for arg in "$@"; do
  case "$arg" in
    --with-k8s) WITH_K8S=1 ;;
    -h|--help)
      sed -n '2,16p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

if [[ -z "${SUDO_USER:-}" ]] && [[ "$EUID" -ne 0 ]]; then
  SUDO="sudo"
else
  SUDO=""
fi

log() { echo -e "\033[1;34m==>\033[0m $*"; }
ok()  { echo -e "\033[1;32m✓\033[0m $*"; }
warn() { echo -e "\033[1;33m!\033[0m $*"; }

log "apt update + core packages"
$SUDO apt update
$SUDO apt install -y \
  openjdk-17-jdk \
  podman podman-compose \
  curl jq bash iproute2 \
  ca-certificates gnupg uidmap flatpak

log "Verify Java 17"
java -version
ok "Java 17 ready"

log "Pin Podman storage (fixes the snap-VS-Code split-storage bug — see CLAUDE.md)"
mkdir -p "${HOME}/.config/containers"
STORAGE_CONF="${HOME}/.config/containers/storage.conf"
if ! grep -q '^graphroot' "$STORAGE_CONF" 2>/dev/null; then
  cat >> "$STORAGE_CONF" <<EOF
[storage]
graphroot = "${HOME}/.local/share/containers/storage"
EOF
  ok "Pinned graphroot in $STORAGE_CONF"
else
  ok "graphroot already pinned — skipping"
fi

log "Podman Desktop (Flatpak)"
if ! flatpak remote-list 2>/dev/null | grep -q '^flathub'; then
  flatpak remote-add --user --if-not-exists flathub https://flathub.org/repo/flathub.flatpakrepo
fi
if flatpak list --user 2>/dev/null | grep -q 'io.podman_desktop.PodmanDesktop'; then
  ok "Podman Desktop already installed — skipping"
else
  flatpak install --user -y flathub io.podman_desktop.PodmanDesktop
fi

if [[ "$WITH_K8S" == "1" ]]; then
  log "Terraform (HashiCorp APT repo)"
  if ! command -v terraform >/dev/null 2>&1; then
    wget -qO- https://apt.releases.hashicorp.com/gpg \
      | $SUDO gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
    echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(. /etc/os-release && echo "$VERSION_CODENAME") main" \
      | $SUDO tee /etc/apt/sources.list.d/hashicorp.list >/dev/null
    $SUDO apt update && $SUDO apt install -y terraform
  fi
  terraform version
  ok "Terraform ready"

  log "Docker (required by kind)"
  if ! command -v docker >/dev/null 2>&1; then
    $SUDO apt install -y docker.io
    $SUDO usermod -aG docker "${USER}"
    warn "Re-login or 'newgrp docker' for docker group to take effect"
  fi
  docker --version
  ok "Docker ready"

  log "kind"
  if ! command -v kind >/dev/null 2>&1; then
    [[ "$(uname -m)" == "aarch64" ]] && ARCH=arm64 || ARCH=amd64
    $SUDO curl -fsSL -o /usr/local/bin/kind "https://kind.sigs.k8s.io/dl/latest/kind-linux-$ARCH"
    $SUDO chmod +x /usr/local/bin/kind
  fi
  kind version
  ok "kind ready"

  log "kubectl"
  if ! command -v kubectl >/dev/null 2>&1; then
    [[ "$(uname -m)" == "aarch64" ]] && ARCH=arm64 || ARCH=amd64
    $SUDO curl -fsSL -o /usr/local/bin/kubectl \
      "https://dl.k8s.io/release/$(curl -sL https://dl.k8s.io/release/stable.txt)/bin/linux/$ARCH/kubectl"
    $SUDO chmod +x /usr/local/bin/kubectl
  fi
  kubectl version --client
  ok "kubectl ready"

  log "Helm 3"
  if ! command -v helm >/dev/null 2>&1; then
    curl -fsSL https://baltocdn.com/helm/signing.asc | $SUDO gpg --dearmor -o /usr/share/keyrings/helm.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/helm.gpg] https://baltocdn.com/helm/stable/debian/ all main" \
      | $SUDO tee /etc/apt/sources.list.d/helm-stable-debian.list >/dev/null
    $SUDO apt update && $SUDO apt install -y helm
  fi
  helm version
  ok "Helm ready"

  log "Python 3 (k8s deploy script)"
  $SUDO apt install -y python3
  python3 --version
fi

log "Done."
ok "Podman-path prereqs installed"
[[ "$WITH_K8S" == "1" ]] && ok "k8s-path prereqs installed"
warn "Re-login (or 'newgrp docker' if --with-k8s) for group changes to take effect."
echo "Verify with section 6 ('Verify Everything') in HOW-TO-RUN-THIS-POC.md."