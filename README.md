# Cathode

TLTV reference server. Multi-channel live TV over HLS with TLTV federation protocol support.

Cathode handles the core broadcast pipeline: multi-channel playout with per-channel GStreamer engines, playlist scheduling, day-based program blocks, direct HLS output via hlssink2, multiple simultaneous outputs (HLS, RTMP simulcast, recording), and peer-to-peer federation (signed metadata, gossip discovery, HLS relay).

## Quick Start

```bash
git clone https://github.com/tltv-org/cathode.git
cd cathode

cp .env.example .env
# Edit .env — set API_KEY

docker compose up -d
```

On first run, cathode generates a failover video and channel slate, creates an Ed25519 keypair for federation, and starts streaming. The API is at port 8888 via traefik. Interactive docs at `/docs`.

See [SETUP.md](SETUP.md) for TLS, federation, and private channels.

### First run

On startup, cathode generates system videos if missing (failover + slate), loads the failover video on the safety layer, and begins streaming. No media scan, no auto-playlist — just clean slate until you configure content.

**Boot priority for input_a:**
1. **Persisted state** — if a named playlist was active before restart, it's restored
2. **Channel slate** — auto-generated, loops until real content is loaded

To add content, upload media and create a playlist via the API or UI:

```bash
# Add media files
cp ~/my-videos/*.mp4 media/

# Or use the API
POST /api/playlists/my-show  { "files": ["clip1.mp4", "clip2.mp4"] }
POST /api/playlists/my-show/load
```

### What's running

| Service | Port | Purpose |
|---|---|---|
| traefik | 8888 | Reverse proxy — routes phosphor + cathode on one port |
| phosphor | (internal) | Web frontend (viewer + management dashboard) |
| cathode | (internal) | Playout engine + REST API + HLS serving |

Traefik routes `/api/*`, `/hls/*`, `/tltv/*`, `/docs` to cathode. Everything else goes to phosphor. For TLS, put Caddy or nginx in front of port 8888.

## Architecture

Interpipeline GStreamer playout engine. Each input channel owns its own
GStreamer pipeline, connected to a central mixer via shared-memory inter
elements. The mixer composites raw A/V and writes to output inter sinks.
Independent OutputLayer pipelines handle encoding and delivery. Source
and output pipelines are fully isolated — hot-swapping or crashing
a source cannot deadlock or affect other pipelines.

```
                    Source Pipelines (independent, configurable layer count)
                    ────────────────────────────────────────────────────────

  ┌─ FAILOVER (file loop) ──────────────────────────────────────────┐
  │  playbin3 → tee → fakesink(sync) ──→ [clock sync]              │
  │                  → queue ───────────→ intervideosink (ch0-video) │
  │             tee → fakesink(sync) ──→ [clock sync]               │
  │                  → queue ───────────→ interaudiosink (ch0-audio) │
  └─────────────────────────────────────────────────────────────────┘
                                        ↕ shared memory
  ┌─ INPUT_A (playlist — gapless) ──────────────────────────────────┐
  │  playbin3 → tee → fakesink(sync) ──→ [clock sync]              │
  │                  → queue ───────────→ intervideosink (ch1-video) │
  │             tee → fakesink(sync) ──→ [clock sync]               │
  │                  → queue ───────────→ interaudiosink (ch1-audio) │
  └─────────────────────────────────────────────────────────────────┘
                                        ↕ shared memory
  ┌─ INPUT_B (HLS/plugin source) ───────────────────────────────────┐
  │  uridecodebin → decode → clocksync → intervideosink (ch2)       │
  │                                    → interaudiosink (ch2)        │
  └─────────────────────────────────────────────────────────────────┘
                                        ↕ shared memory
  ┌─ BLINDER ───────────────────────────────────────────────────────┐
  │  videotestsrc (is-live) ─────────→ intervideosink (ch3-video)   │
  │  audiotestsrc (is-live) ─────────→ interaudiosink (ch3-audio)   │
  └─────────────────────────────────────────────────────────────────┘


                    Mixer Pipeline (one, static — raw A/V output)
                    ─────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────────────────┐
  │                                                                 │
  │  intervideosrc (ch0) ─┐      ┌──→ capsfilter                   │
  │  intervideosrc (ch1) ─┼──→ compositor   ↓                      │
  │  intervideosrc (ch2) ─┤      │     [plugin overlays]            │
  │  intervideosrc (ch3) ─┘      │          ↓                      │
  │                              │   intervideosink (mix-video)     │
  │  interaudiosrc (ch0) ─┐      │                                  │
  │  interaudiosrc (ch1) ─┼──→ audiomixer ──→ interaudiosink        │
  │  interaudiosrc (ch2) ─┤                    (mix-audio)          │
  │  interaudiosrc (ch3) ─┘                                         │
  └─────────────────────────────────────────────────────────────────┘
                                        ↕ shared memory

                    Output Pipelines (independent, multiple)
                    ────────────────────────────────────────

  ┌─ HLS Output ───────────────────────────────────────────────────┐
  │  intervideosrc (mix-video) → x264enc → h264parse → hlssink2    │
  │  interaudiosrc (mix-audio) → avenc_aac ─────────↗              │
  │                                     → /data/hls/{channel_id}/  │
  └─────────────────────────────────────────────────────────────────┘

  ┌─ RTMP Simulcast (optional) ────────────────────────────────────┐
  │  intervideosrc (mix-video) → x264enc → flvmux → rtmpsink      │
  │  interaudiosrc (mix-audio) → avenc_aac ──────↗                 │
  │                                     → rtmp://twitch.tv/...     │
  └─────────────────────────────────────────────────────────────────┘
```

Key design properties:
- **Zero-gap playlists** — playbin3 gapless playback with tee+fakesink clock sync
- **Multiple outputs** — HLS, RTMP simulcast, file recording simultaneously
- **Crash isolation** — a source or output pipeline failure cannot take down the mixer
- **Safe teardown** — `pipeline.set_state(NULL)` is atomic per pipeline; no deadlocks
- **Clean lifecycle** — no element accumulation for long-running playlists
- **Data-driven layers** — configurable layer count and roles (safety, content, override, overlay)

## Plugins

Cathode works with zero plugins installed. All plugins are optional and live in a separate repo. See [cathode-plugins](https://github.com/tltv-org/cathode-plugins) for available plugins and installation instructions.

## API

Interactive API docs at `/docs` (Swagger UI) and `/openapi.json`. All `/api/*` endpoints require an API key — pass via `X-API-Key` header or `?api_key=` query param. Protocol endpoints are always public.

For the full endpoint reference, see [docs/API.md](docs/API.md).

## Federation

TLTV federation protocol (v1): Ed25519 signed metadata, gossip-based peer discovery, HLS segment relay, private channel access tokens, `tltv://` URI scheme. Full spec at [tltv-org/protocol](https://github.com/tltv-org/protocol).
