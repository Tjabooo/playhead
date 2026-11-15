# playhead

[![CI](https://github.com/Tjabooo/playhead/actions/workflows/ci.yml/badge.svg)](https://github.com/Tjabooo/playhead/actions/workflows/ci.yml) [![Release](https://img.shields.io/github/v/release/Tjabooo/playhead?include_prereleases&sort=semver)](https://github.com/Tjabooo/playhead/releases) [![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A BitTorrent engine built for streaming: it downloads around the playhead first, lets you seek
anywhere, and never sends a byte outside the VPN interface you bind it to.

Most clients treat streaming as "sequential download". That breaks in two ways: one slow peer
holding an early piece freezes playback for a minute or more, and seeking past the downloaded
part just waits until the download gets there. playhead has a deadline piece picker that knows
where every player is reading, so it can work around slow peers and jump with a seek.

## Features

- **Deadline piece picker.** Every HTTP reader is a playhead. The next few MiB are *critical*,
  the next 32 MiB are the *window*, and everything else is *background*.
  - Critical and window pieces go to fast peers only. Slow or snubbed peers get background work.
  - A critical block that's late, compared with what its peer's rate predicts, is requested again
    from a faster peer. The first copy wins and the others are cancelled.
  - Peers that stop delivering are snubbed within seconds, and their blocks are handed to others.
  - Seeking moves the window and cancels background requests, so the pipelines refill with the
    new position right away.
- **Seekable HTTP range server.** `GET /file/<n>` supports `Range` (including suffix ranges).
  A read of a missing piece waits until the piece arrives and passes its hash check, so a player
  never sees a hole. Point mpv, VLC, ffmpeg or a browser at it.
- **Fail-closed VPN binding.** With `--bind-interface wg0`, every TCP and UDP socket (peers,
  trackers, DHT, DNS) is bound to that interface. On Linux this also sets `SO_BINDTODEVICE`.
  - The interface is checked twice a second. If it disappears or its address changes, every
    connection closes, nothing reconnects, and no tracker or DHT traffic goes out until it's back.
  - DNS for tracker and DHT host names goes through the tunnel, never to the system resolver.
  - If the interface is missing at start, playhead refuses to start. Running without a binding
    needs an explicit `--unbound`.
  - `playhead leak-test` checks a real setup.
- **No executables or archives.** `.exe`, scripts, disk images and archives (incl. split RARs)
  are never requested, written or served. A fake "episode" that's really `Show.mkv.exe` can't
  be started at all.
- Magnet links (hex or base32) with metadata download from peers (BEP 9/10), `.torrent` files,
  HTTP and UDP trackers (BEP 3/15/23), and the mainline DHT in read-only mode (BEP 5/43).
- Resumes: pieces already on disk are verified and reused.
- Uploads back to the swarm (4 slots plus an optimistic one), or not, with `--no-upload`.
- One small static binary for Linux and Windows.

## Install

From a [release](https://github.com/Tjabooo/playhead/releases), or build it yourself:

```sh
cargo install --git https://github.com/Tjabooo/playhead
```

## Usage

```sh
# Stream the largest video file, bound to a WireGuard interface
playhead stream 'magnet:?xt=urn:btih:...' --bind-interface wg0 --out ~/Downloads
# prints http://127.0.0.1:8888/file/0/<name>; open it in a player
mpv http://127.0.0.1:8888/stream

# List the files first, then pick one
playhead files 'magnet:?xt=urn:btih:...' --bind-interface wg0
playhead stream my.torrent --file 3 --bind-interface wg0

# Check the binding on this machine
playhead leak-test --bind-interface wg0
```

`leak-test` output looks like this:

```
PASS bound to 10.64.12.34 (Interface("wg0"))
PASS DNS through the tunnel: api.ipify.org -> 104.26.12.205
PASS public IP through the tunnel 185.65.134.1, without it 203.0.113.7
PASS kill switch: no TCP, UDP or DNS while the tunnel is down
```

### Daemon mode (for media servers)

`playhead serve` runs one process for many streams behind a token-protected JSON API on a
loopback address. A media server starts it once and asks it for streams:

```sh
PLAYHEAD_TOKEN=$(openssl rand -hex 24) playhead serve --bind-address 10.64.12.34 \
  --api 127.0.0.1:9700 --cache-dir /var/cache/playhead
```

| Route | What |
| --- | --- |
| `GET /api/health` | Version, network guard state, stream count |
| `POST /api/streams` | Start (or reuse) a stream: `{"torrent": "<base64 .torrent>" \| "magnet": … \| "info_hash": …, "file": 0, "trackers": [], "peers": [], "assist": true, "external_paths": [], "states": "0122…"}` |
| `GET /api/streams[/<id>]` | Status, plus `"own"`: the pieces this stream downloaded itself |
| `PUT /api/streams/<id>/external` | The other client's piece states and file paths (assist mode) |
| `PUT /api/streams/<id>/peers` | More peers |
| `DELETE /api/streams/<id>` | Stop and delete its cache |
| `GET /s/<id>/file/<n>` | The stream (range requests) |

Every request needs `Authorization: Bearer <token>`. Streams nobody asks about for 10 minutes
are closed.

**Assist mode** (`"assist": true`) is for running next to another client that already downloads
the torrent in order, like qBittorrent, without taking it over:
- Send playhead that client's piece states (qBittorrent's `pieceStates`: 0 missing, 1 downloading,
  2 verified) and where it keeps the file.
- Pieces the other client has are read from its file. Each one is hash-checked once, and a bad
  one is downloaded after all.
- Pieces it's busy with are left to it unless a reader needs them right now.
- playhead downloads only what a reader needs next that the other client doesn't have yet: the
  start, a seek target, a piece stuck on a slow peer.
- Nothing is downloaded in the background, and the other client stays the one that finishes,
  seeds and keeps the file.

### HTTP API (single stream)

| Route | What |
| --- | --- |
| `GET /` | Torrent name and file list with stream URLs (JSON) |
| `GET\|HEAD /file/<n>[/<name>]` | The file, with range support; selecting it starts the download |
| `GET /stream` | The selected file |
| `GET /status` | Engine state: network guard, rate, peers, pieces, readers and how far ahead each one is buffered, duplicate requests, wasted bytes |

## Configuration

| Flag | Default | What |
| --- | --- | --- |
| `--bind-interface NAME` / `--bind-address IP` / `--unbound` | (required) | Where traffic may go |
| `--dns IP[:PORT]` | `1.1.1.1`, `9.9.9.9` | Resolvers used through the tunnel. Some VPN apps block other resolvers: use the VPN's own (Mullvad: `10.64.0.1`) |
| `--http ADDR` | `127.0.0.1:8888` | HTTP server address |
| `--out DIR` | `.` | Download folder |
| `--port N` | random | Incoming peer port. Use your VPN's forwarded port if it has one |
| `--window-mb N` | 32 | Read-ahead window per playhead |
| `--picker deadline\|sequential` | `deadline` | `sequential` is the naive baseline used by the benchmark |
| `--tracker URL`, `--peer IP:PORT` | | Extra trackers and peers |
| `--no-dht`, `--no-upload`, `--max-peers N` | | |
| `--allow-ext EXT`, `--block-ext EXT` | | Adjust the file block list |

Log level: `PLAYHEAD_LOG=debug|info|warn|error|quiet`.

On Windows, `--bind-address` with the tunnel's IP is the reliable choice. Windows' strong host
model keeps sockets bound to the tunnel address inside the tunnel. When the tunnel goes away,
its address disappears and the guard trips.

## Benchmark

`playhead bench` builds a local swarm and plays a 1 GiB, 0.78 MB/s video through it with a
simulated player: 2 s start buffer, 30 s read-ahead, 60 s of playback, a seek to 60 % and 25 s more.
`--suite` runs five swarms: four fast seeders sharing 1.5 MB/s (*healthy*), plus a 12 kB/s
seeder (*slow-peer*), plus a seeder that unchokes and never answers (*stuck-peer*), six seeders
that each have about 60 % of the pieces (*patchy*), and four uncapped seeders (*fast*).

`--client qbittorrent` runs the same swarm against a real qBittorrent (5.3.0, libtorrent 2.1.2
here). It's driven the way a media server's "play while downloading" does: sequential download
and first/last piece priority, with reads held until `pieceStates` says the piece is verified.
`bench/qbittorrent-test.sh` starts a throwaway headless one.

Median of two runs each (`playhead bench --suite --runs 2 --client qbittorrent --client playhead`):

| Swarm | Start: qBittorrent → playhead | Stalls while playing | Seek to 60 %, playing again |
| --- | --- | --- | --- |
| healthy | 28 s → **1.4 s** | 3 (9 s) → **0** | 367 s → **2.0 s** |
| slow-peer | 27 s → **2.6 s** | 5 (126 s, longest 68 s) → **0** | 300 s → **2.0 s** |
| stuck-peer | 68 s → **2.6 s** | 2 (6 s) → **0** | 320 s → **2.0 s** |
| patchy | 27 s → **2.5 s** | 4 (15 s) → **0** | 357 s → **2.0 s** |
| fast | 8 s → **0.1 s** | 0 → 0 | 3 s → **0 s** (whole file: 130 s → 85 s) |

qBittorrent's seek waits for the sequential download to get there. playhead moves its window and
has the new spot in about two seconds.

Real swarm, a Debian netinst ISO (756 MiB) through a Mullvad WireGuard interface: the whole file
in 71 s with a matching SHA256, cold seeks returning 4 MiB in 2.7–3.8 s, and 0.6 % of the data
fetched twice.

## How it works

```
 player ──HTTP range──▶ http.rs ──playhead position──▶ engine actor ──requests──▶ peer tasks ──▶ swarm
                          ▲ waits for verified piece        │  picker.rs (deadline)       │
                          └──────────── storage.rs ◀────────┘  storage, hashing           │
                                                         net.rs: NetGuard (bind + kill switch) for
                                                         every socket: peers, trackers, DHT, DNS
```

- `engine.rs`: one actor owns the picker, storage and peer table. Peers, trackers, DHT and the
  listener are tasks that talk to it over channels.
- `picker.rs`: the deadline picker described above, plus snubbing, duplicate requests and
  cancellation, and endgame.
- `net.rs`: `NetGuard`. Every socket is created here, bound to the tunnel, and checked against the
  current state. A watch channel tells every connection to close the moment the tunnel changes.
- `tests/swarm.rs` runs end-to-end tests on a local swarm (`testkit.rs`). They cover streaming via
  a magnet link, ranges and seeking, a stuck peer, the exe blocker, the kill switch, and a leak
  test: bound to `127.0.0.2`, every connection that reaches the seeders and both trackers must
  come from that address.

Not yet: uTP, PEX, HTTPS trackers, IPv6 peers when bound to an IPv4 tunnel, and BitTorrent v2.

## License

MIT
