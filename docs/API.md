# API Reference

Interactive docs are available at `/docs` (Swagger UI) and `/openapi.json`.

All `/api/*` endpoints require an API key when `API_KEY` is set — pass via
`X-API-Key` header or `?api_key=` query param. Protocol endpoints are always
public.

**Multi-channel:** All per-channel endpoints accept `?channel={id}`. When
omitted, the default (first) channel is used.

## Endpoints

```bash
# Channels
GET  /api/channels                  # List all channels
POST /api/channels                  # Create a new channel
GET  /api/channels/{id}             # Channel detail + encoding
PATCH /api/channels/{id}            # Update channel metadata
DELETE /api/channels/{id}           # Delete channel (not last)

# Status
GET  /api/status                    # System status + schedule
GET  /api/now-playing               # Current clip
GET  /api/system                    # CPU/RAM/disk
GET  /api/media                     # Available media files (global)

# Named playlists (per-channel)
GET  /api/playlists                 # List all saved playlists
POST /api/playlists/{name}          # Create/update playlist
GET  /api/playlists/{name}          # Get playlist by name
DELETE /api/playlists/{name}        # Delete playlist
POST /api/playlists/{name}/load     # Load on a layer

# Playlist (legacy)
POST /api/playlist                  # Set today's playlist
POST /api/skip                      # Next clip
POST /api/back                      # Previous clip

# Schedule
GET  /api/schedule                  # 7-day view
POST /api/schedule/generate         # Auto-generate for a date

# Program (per-channel)
GET  /api/program/{date}            # Day's program
POST /api/program/{date}            # Set program blocks
GET  /api/program/block-types       # Available block types (core + plugin)

# Output pipelines (per-channel)
GET  /api/outputs                   # List all active outputs
POST /api/outputs                   # Create output (hls, rtmp, file, null)
GET  /api/outputs/{name}            # Output status
PATCH /api/outputs/{name}           # Update config (stop + rebuild)
DELETE /api/outputs/{name}          # Stop and remove output

# Playout engine (per-channel)
GET  /api/playout/health            # Engine health + per-layer visibility
POST /api/playout/restart           # Restart engine
GET  /api/playout/layers/config     # Layer configuration + presets
PUT  /api/playout/layers/config     # Set layers (preset or custom)
GET  /api/playout/layers/{name}     # Layer status
POST /api/playout/layers/{name}/source  # Load source on a layer
POST /api/playout/layers/{name}/show    # Show layer (alpha/volume)
POST /api/playout/layers/{name}/hide    # Hide layer
POST /api/playout/layers/{name}/position     # PIP position/size
DELETE /api/playout/layers/{name}/position   # Reset to full-screen

# Playout settings (per-channel)
GET  /api/playout/mode              # Scheduling mode + day_start
POST /api/playout/mode              # Switch loop/schedule, set day_start
POST /api/playout/schedule/resync   # Clear manual overrides, resync to program
GET  /api/playout/encoding          # Resolution, fps, bitrate
PATCH /api/playout/encoding         # Update encoding (restarts engine)
GET  /api/playout/failover          # Failover video config
PATCH /api/playout/failover         # Update failover settings
POST /api/playout/failover/regenerate  # Regenerate failover video
GET  /api/playout/slate             # Slate video config
PATCH /api/playout/slate            # Update slate settings
POST /api/playout/slate/regenerate  # Regenerate slate video

# Media library (global)
GET  /api/media                     # List all files (relative paths)
POST /api/media/upload              # Upload (optional ?folder=)
POST /api/media/mkdir               # Create subfolder
GET  /api/media/{path}              # File metadata
DELETE /api/media/{path}            # Delete file

# Plugins
GET  /api/plugins                   # List all plugins (loaded + disabled)
POST /api/plugins/{name}/enable     # Enable plugin (restart required)
POST /api/plugins/{name}/disable    # Disable plugin (restart required)
GET  /api/plugins/{name}/presets    # List presets for a plugin
POST /api/plugins/{name}/generate   # Render source to media file

# Logs
GET  /api/logs                      # Recent log entries (ring buffer)
GET  /api/logs/stream               # Real-time SSE log stream

# Backup / restore
POST /api/backup                    # Download tar.gz of all config + state
POST /api/restore                   # Upload tar.gz to restore

# Channel metadata (per-channel)
GET  /api/channel                   # Federation identity fields
PATCH /api/channel                  # Update metadata

# Federation management
GET  /api/peers                     # Known peers
POST /api/peers                     # Add a peer
GET  /api/relay                     # Active relays
POST /api/relay                     # Start relaying a channel
GET  /api/tokens/{channel}          # List access tokens
POST /api/tokens/{channel}          # Create access token

# TLTV protocol (public, no auth)
GET  /.well-known/tltv              # Node info (all channels)
GET  /tltv/v1/channels/{id}         # Signed channel metadata
GET  /tltv/v1/channels/{id}/stream.m3u8  # Stream endpoint
GET  /tltv/v1/channels/{id}/guide.json   # Program guide
GET  /tltv/v1/peers                 # Peer exchange
```

## Source types

For `POST /api/playout/layers/{name}/source`:

| Type | Parameters | Description |
|------|-----------|-------------|
| `test` | `pattern`, `wave` | Test pattern (smpte, black, snow, etc.) |
| `failover_file` | — | Load the failover video from storage settings |
| `failover_debug` | — | Live SMPTE bars with channel info (no file) |
| `file_loop` | `path` | Single file looping forever |
| `playlist` | `entries`, `loop` | Clip sequence with optional looping |
| `hls` | `url` | Pull from remote HLS stream |
| `image` | `path` | Static image (infinite frames) |
| `disconnect` | — | Remove current source |
| (plugin) | varies | Plugin source types via registry |

## Schedule block types

For `POST /api/program/{date}`:

| Type | Description |
|------|-------------|
| `playlist` | Play a named playlist or file list |
| `file` | Play a single file |
| `image` | Show a static image |
| `redirect` | Play an HLS stream (another channel, external feed) |
| (plugin) | Plugin block types via registry |

## Layer presets

For `PUT /api/playout/layers/config`:

| Preset | Layers | Description |
|--------|--------|-------------|
| `standard` | failover, input_a, input_b, blinder | Default 4-layer broadcast |
| `minimal` | failover, content | 2-layer minimal |
| (custom) | user-defined | Any combination of roles |
