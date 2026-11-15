# Changelog

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
