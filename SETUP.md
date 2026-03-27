# Setup & Deployment

Production deployment guide. Covers first-run setup, TLS, federation,
private channels, and backup.

## Prerequisites

- Docker Engine 24+ with Compose v2
- A domain name pointed at the server (for federation/TLS)
- Port 443 reachable from the internet (for federation)

Hardware (minimum for a single 1080p30 channel):

| Resource | Minimum | Recommended |
|---|---|---|
| CPU | 2 cores | 4 cores |
| RAM | 2 GB | 4 GB |
| Disk | 10 GB + media | SSD, 50 GB+ |

Encoding dominates CPU. Add ~1 core per additional concurrent channel.

## Quick start

```bash
git clone https://github.com/tltv-org/cathode.git
cd cathode

cp .env.example .env
# Edit .env — set API_KEY

docker compose up -d
```

On first run, cathode generates a test card (SMPTE bars with channel name),
an Ed25519 keypair for federation, and starts streaming.

## Configuration

### Environment variables (.env)

| Variable | Required | Default | Description |
|---|---|---|---|
| `API_KEY` | Yes | — | Management API key (see [API authentication](#api-authentication)) |
| `TZ` | No | `UTC` | Server timezone (IANA name) |
| `CORS_ORIGINS` | No | — | Comma-separated allowed origins (see [CORS](#cors)) |
| `LOG_LEVEL` | No | `INFO` | Log level (DEBUG, INFO, WARNING, ERROR) |

Generate an API key:

```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

Most settings are configured via channel YAML files, not environment
variables. The env file is for secrets and server-level config only.

### Channel configuration

Channel YAML files live in `config/channels/`. On first start, cathode
copies `channel-one.example.yaml` to `channel-one.yaml` if no config
exists.

```yaml
# config/channels/channel-one.yaml
id: channel-one
display_name: "My Station"
timezone: "UTC"

identity:
  private_key_path: "/data/keys/channel-one.key"
  description: "24/7 community television"
  language: "en"
  tags: ["community", "local"]
  access: "public"          # "public" or "token"
  origins:                  # Public hostnames for federation
    - "tv.example.com:443"

output:
  type: "hls"

media:
  base_dir: "/media"
  program_dir: "/data/programs"
```

### Adding media

```bash
cp ~/videos/*.mp4 media/
```

Or upload via the API:

```bash
curl -X POST -H "X-API-Key: $API_KEY" \
  -F "file=@intro.mp4" \
  http://localhost:8888/api/media/upload
```

## TLS

The TLTV protocol requires TLS for all protocol endpoints when serving
remote clients (PROTOCOL.md section 8.8). Put a reverse proxy in front
of cathode that handles TLS termination.

**Caddy** (automatic Let's Encrypt):

```
# Caddyfile
tv.example.com {
    reverse_proxy localhost:8888
}
```

**nginx** (manual certs):

```nginx
server {
    listen 443 ssl;
    server_name tv.example.com;

    ssl_certificate     /etc/letsencrypt/live/tv.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/tv.example.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:8888;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-Proto https;
    }
}
```

## API authentication

The management API (`/api/*`) is protected by an API key when the `API_KEY`
environment variable is set. Protocol endpoints (`/tltv/v1/*`,
`/.well-known/tltv`) are always public.

Set the key in `.env`:

```
API_KEY=your-secret-key-here
```

Include it in requests:

```bash
# Header (preferred)
curl -H "X-API-Key: your-secret-key-here" http://localhost:8888/api/status

# Query parameter
curl "http://localhost:8888/api/status?api_key=your-secret-key-here"
```

Rate limiting: 120 requests per 60 seconds per IP on `/api/*` endpoints.

### CORS

Cross-origin requests are blocked by default (`CORS_ORIGINS` is empty).
Set this to the origin(s) that need API access:

```
CORS_ORIGINS=https://tv.example.com,https://admin.example.com
```

### Security headers

Cathode sets the following headers on all responses:

- `Content-Security-Policy` — restrictive default (self-only scripts,
  styles, media, connections)
- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY`
- `Referrer-Policy: strict-origin-when-cross-origin` (overridden to
  `no-referrer` for private channels)

No additional header configuration is needed in your reverse proxy.

### Request size limits

JSON API request bodies are limited to 1 MB. Media uploads and backup
restore have their own limits (2 GB and 100 MB respectively).

## Federation

Federation lets other TLTV nodes discover, verify, and relay your channel.

### Channel identity

On first start, cathode generates an Ed25519 keypair at
`data/keys/<channel-id>.key`. The public key becomes the channel's
federation ID (a `TV`-prefixed base58 string). This ID is permanent
and globally unique — it is the channel's identity on the network.

**Back up your private key.** If you lose it, you lose the channel
identity. The only recovery path is key migration (section 5.14),
which requires the old key.

```bash
cp data/keys/channel-one.key ~/backup/channel-one.key.bak
```

### Origins

Set `origins` in the channel config to the public hostname(s) where your
node is reachable. These are included in signed metadata so other nodes
know where to find you.

```yaml
identity:
  origins:
    - "tv.example.com:443"
```

Without origins, peer discovery and relay still work via gossip, but
direct resolution requires at least one origin.

### Peer exchange

Cathode automatically discovers other channels via gossip-based peer
exchange. Peers are exchanged every 30 minutes. The peer store persists
to `data/peers.json`.

To manually add a peer:

```bash
curl -X POST -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"hint": "other-node.example.com:443"}' \
  http://localhost:8888/api/peers/add
```

### Relaying other channels

To relay another channel's stream through your node:

```bash
curl -X POST -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"channel_id": "TVxxxx...", "hints": ["origin.example.com:443"]}' \
  http://localhost:8888/api/relay/add
```

The relay fetches signed metadata (verifying the Ed25519 signature), then
continuously polls HLS segments from the upstream origin. Relayed channels
appear in your node's `/.well-known/tltv` and peer exchange responses.

## Private channels

Set `access: "token"` in the channel config to require token authentication
for all endpoints (metadata, stream, guide).

```yaml
identity:
  access: "token"
```

Manage tokens via the API:

```bash
# Create a token (never expires)
curl -X POST -H "X-API-Key: $API_KEY" \
  http://localhost:8888/api/tokens/create

# Create a token with expiry
curl -X POST -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"expires": "2026-12-31T23:59:59Z"}' \
  http://localhost:8888/api/tokens/create

# List tokens
curl -H "X-API-Key: $API_KEY" http://localhost:8888/api/tokens

# Revoke a token
curl -X DELETE -H "X-API-Key: $API_KEY" \
  http://localhost:8888/api/tokens/<token-id>
```

Clients access the channel with the token in the query string:

```
tltv://TVxxxx...?token=<token>
https://tv.example.com/tltv/v1/channels/TVxxxx.../stream.m3u8?token=<token>
```

Private channels are excluded from peer exchange and `/.well-known/tltv`.
HLS manifests are generated per-request with the token embedded in every
segment URI. All responses include privacy headers to prevent token leakage.

## Data persistence

All persistent state lives under `data/`:

| Path | Contents |
|---|---|
| `data/keys/` | Ed25519 private keys (critical — back up) |
| `data/hls/` | HLS segments and m3u8 playlists (per channel) |
| `data/playlists/` | Playlist persistence (date-indexed + named) |
| `data/playout-state/` | Per-channel active playlist state (survives restarts) |
| `data/programs/` | Day-based program schedules |
| `data/peers.json` | Discovered peer channels |
| `data/relays.json` | Relay configuration |
| `data/tokens/` | Private channel access tokens |
| `data/seq/` | Sequence counters for signed documents |
| `data/migrations/` | Migration documents |

The `data/` directory is mounted as a Docker volume. If the container
is recreated, all state persists.

**What to back up:**
- `data/keys/` — channel identity. Loss is unrecoverable without migration.
- `config/channels/*.yaml` — channel configuration.
- `media/` — your media files (if not stored elsewhere).

**What's safe to lose:**
- `data/peers.json` — repopulated via gossip.
- `data/seq/` — regenerated from current time.
- `data/relays.json` — reconfigure via API.

## Multiple channels

Add another YAML file to `config/channels/`:

```bash
cp config/channels/channel-one.example.yaml config/channels/channel-two.yaml
# Edit channel-two.yaml — change id, display_name
```

Each channel gets its own Ed25519 keypair, federation ID, program schedule,
metadata endpoint, and HLS output directory.

## Plugins

Plugins add optional features (HTML rendering, media generation, overlays,
script sources). They live in a separate repo:

```bash
git clone https://github.com/tltv-org/cathode-plugins.git cathode-plugins
```

Mount into the cathode container:

```yaml
# docker-compose.yml cathode service volumes:
- ./cathode-plugins:/app/plugins
```

See [cathode-plugins](https://github.com/tltv-org/cathode-plugins)
for the full list and per-plugin documentation.

## Troubleshooting

### Stream not starting

```bash
docker compose ps
docker compose logs cathode --tail 50
```

### Federation not working

```bash
# Check your node's well-known endpoint
curl http://localhost:8888/.well-known/tltv

# Verify metadata is being signed
curl http://localhost:8888/tltv/v1/channels/<your-channel-id>

# Check peer store
curl -H "X-API-Key: $API_KEY" http://localhost:8888/api/peers
```

Common causes:
- No TLS (other nodes won't connect over plain HTTP)
- `origins` not set in channel config
- Firewall blocking port 443
