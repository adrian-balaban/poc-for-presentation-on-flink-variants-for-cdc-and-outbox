# Visual Studio Code on Ubuntu Server LTS

**Note:** Visual Studio (full Microsoft IDE) doesn't exist for Linux — use Visual Studio Code instead.

## Option 1: apt repository (needs desktop/GUI)

**Quick snap method:**
```bash
sudo snap install code --classic
```

**Or via Microsoft apt repo:**
```bash
sudo apt-get update
sudo apt-get install -y wget gpg apt-transport-https

wget -qO- https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > packages.microsoft.gpg
sudo install -D -o root -g root -m 644 packages.microsoft.gpg /etc/apt/keyrings/packages.microsoft.gpg
echo "deb [arch=amd64,arm64,armhf signed-by=/etc/apt/keyrings/packages.microsoft.gpg] https://packages.microsoft.com/repos/code stable main" | sudo tee /etc/apt/sources.list.d/vscode.list

sudo apt-get update
sudo apt-get install -y code
```

Only works if the server has a desktop environment or X11 forwarding configured.

## Option 2: Headless server (recommended for Server LTS)

### Remote-SSH (from laptop/desktop to server)
- Install VS Code on your local machine
- Add "Remote - SSH" extension
- Connect to server; VS Code installs headless backend automatically over SSH

### VS Code tunnel (browser or local IDE)
On the server:
```bash
curl -Lk 'https://code.visualstudio.com/sha/download?build=stable&os=cli-alpine-x64' --output vscode_cli.tar.gz
tar -xf vscode_cli.tar.gz
./code tunnel
```
Then access via vscode.dev or local VS Code (GitHub/Microsoft sign-in required).

### code-server (browser-based, third-party)
```bash
curl -fsSL https://code-server.dev/install.sh | sh
```
Access at `http://server:8080` in a browser.
