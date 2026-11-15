#!/bin/sh
# Start a throwaway, headless qBittorrent for `playhead bench --client qbittorrent`.
# Usage: bench/qbittorrent-test.sh start|stop [PROFILE_DIR] [WEBUI_PORT] [INTERFACE]
# Uses the installed qbittorrent (GUI build, run with Qt's offscreen platform) or qbittorrent-nox.
# Port forwarding (UPnP/NAT-PMP), DHT, PeX and LSD are off; the WebUI listens on 127.0.0.1 only and
# skips auth for localhost. INTERFACE binds qBittorrent's torrent traffic to one interface (optional).
set -eu
cmd=${1:-start}
profile=${2:-/tmp/playhead-qbt}
port=${3:-8099}
iface=${4:-}
pat="[q]bittorrent.*--profile=$profile"
if [ "$cmd" = stop ]; then
    pkill -TERM -f "$pat" || true
    exit 0
fi
pkill -TERM -f "$pat" 2>/dev/null && sleep 2 || true
rm -rf "$profile"
mkdir -p "$profile/qBittorrent/config" "$profile/downloads"
# The GUI build only starts the WebUI when credentials are set: make a random throwaway one.
pass=$(python3 -c 'import hashlib,os,base64;s=os.urandom(16);k=hashlib.pbkdf2_hmac("sha512",os.urandom(24),s,100000,64);print(base64.b64encode(s).decode()+":"+base64.b64encode(k).decode())')
cat > "$profile/qBittorrent/config/qBittorrent.conf" <<CONF
[LegalNotice]
Accepted=true

[BitTorrent]
Session\\DHTEnabled=false
Session\\PeXEnabled=false
Session\\LSDEnabled=false
Session\\QueueingSystemEnabled=false
Session\\MultiConnectionsPerIp=true
Session\\DefaultSavePath=$profile/downloads
Session\\Interface=$iface
Session\\InterfaceName=$iface
Session\\AnonymousModeEnabled=false

[Network]
PortForwardingEnabled=false

[Preferences]
WebUI\\Enabled=true
WebUI\\Address=127.0.0.1
WebUI\\Port=$port
WebUI\\LocalHostAuth=false
WebUI\\CSRFProtection=false
WebUI\\HostHeaderValidation=false
WebUI\\Username=bench
WebUI\\Password_PBKDF2="@ByteArray($pass)"
General\\StartMinimized=true
CONF
if command -v qbittorrent-nox >/dev/null; then bin="qbittorrent-nox"; else bin="qbittorrent"; fi
cd "$profile"
QT_QPA_PLATFORM=offscreen nohup nice -n 5 "$bin" --profile="$profile" --no-splash > "$profile/qbt.log" 2>&1 &
i=0
until curl -sf "http://127.0.0.1:$port/api/v2/app/version" >/dev/null; do
    i=$((i + 1)); [ $i -gt 30 ] && { echo "qBittorrent WebUI didn't come up, see $profile" >&2; exit 1; }
    sleep 0.5
done
echo "qBittorrent $(curl -s "http://127.0.0.1:$port/api/v2/app/version") ($(curl -s "http://127.0.0.1:$port/api/v2/app/buildInfo" | python3 -c "import json,sys;print(\"libtorrent \"+json.load(sys.stdin)[\"libtorrent\"])")) on http://127.0.0.1:$port"
