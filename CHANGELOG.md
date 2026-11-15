# Changelog

## v0.2.3 (2026-11-15)

- Peer exchange (BEP 11, receiving): peers tell playhead about other peers.
- `start_lead_bytes` per stream: the start of the file is fetched before anyone reads it, far end
  first, so next to a client downloading in order from the front the two meet in the middle.
- New peers start with a 12-block pipeline (was 6): faster ramp-up over high-latency tunnels.

## v0.2.2 (2026-11-15)

- `playhead serve --exit-with-stdin`: exits when its parent's pipe closes, so a daemon started by
  another program never outlives it (Windows doesn't end child processes with their parent).

## v0.2.1 (2026-11-15)

- Assist mode opens the other client's file as soon as it's known: on Windows the first open by a new
  process can take up to a second, which used to land on the first seek.
- `download_limit` (bytes/s) per stream in `playhead serve`.

## v0.2.0 (2026-11-15)

- `playhead serve`: daemon mode, many streams behind a token-protected loopback JSON API.
- Assist mode: run next to another client (qBittorrent) on the same torrent. Pieces it has are
  served from its file (hash-checked, bad ones re-downloaded), pieces it's busy with are left to it,
  and playhead fetches only what readers need next. No background download.
- `playhead bench --client qbittorrent`: the same local swarm against a real qBittorrent, driven
  like a media server's "play while downloading" (sequential + first/last piece priority, reads
  held until pieceStates says verified). `--suite` runs five scenarios; `bench/qbittorrent-test.sh`
  starts a throwaway headless qBittorrent.
- HTTP readers stop when their engine stops instead of waiting for ever.

## v0.1.0 (2026-11-14)

First release: a streaming BitTorrent engine.

- Deadline piece picker: critical, window and background pieces per playhead; slow and snubbed
  peers kept off urgent pieces; late urgent blocks re-requested from faster peers and the other
  copies cancelled; seeks cancel background requests.
- HTTP range server (`/file/<n>`, `/stream`, `/status`); reads of missing pieces wait for them.
- Fail-closed network guard: every peer, tracker, DHT and DNS socket is bound to one interface
  or address; when it goes away every connection closes and nothing reconnects until it's back.
  `playhead leak-test` checks a real setup.
- Executables, disk images and archives are never downloaded, written or served.
- Magnet links + metadata exchange, .torrent files, HTTP/UDP trackers, read-only DHT, resume, uploads.
- `playhead bench`: local swarm benchmark against a naive sequential picker.
