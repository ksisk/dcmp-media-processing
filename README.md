# DCMP Media Processing — the portal

One address for the DCMP Keying Pipeline, Media Forager and the machine they share. A single Caddy container
serves the shell (`site/index.html`) and reverse-proxies the two apps:

| path | goes to | note |
|---|---|---|
| `/` | the shell: tabs **Keying · Forager · Host** | |
| `/keying/…` | keying status page (`KEYING_UPSTREAM`) | prefix stripped; the app also accepts it (`BASE_PATH=/keying`); `/events` streamed |
| `/forager/…` | Media Forager (`FORAGER_UPSTREAM`) | prefix stripped; the page computes its base from its URL; forager's own basic auth passes through |

The Keying and Forager tabs frame each app with `?embed=1`, which hides the app's own header; the portal keeps the
app's current page in the hash (`#keying/stats`) so refresh and bookmarks return to the same place. "Open … on its
own ↗" opens the app at its own address. The **Host** tab shows what each app is doing, the live monitor (CPU,
per core, every GPU's load and VRAM, temperatures), a GPUs table with the job each app has on each card, and the
GPU policy editor — all from the GPU broker below.

## GPU broker (`broker/broker.py`)

A second container in the same compose stack (`python:3.12-slim`, stdlib only, port 8090, also reachable as
`/host/api/*` through Caddy) decides which card every GPU job runs on, for every app:

| call | who | what |
|---|---|---|
| `POST /api/claim {app, job, type}` | an app starting a job | returns the card (`uuid`, `index`, `short`, `lease`) or `{wait: true, reason}` when no allowed card has the VRAM — ask again later |
| `POST /api/heartbeat {lease}` | the app, once a minute | keeps the lease; a lease without a heartbeat for `LEASE_TTL` (600 s) is dropped, so a crashed job never pins a card |
| `POST /api/release {lease}` | the app when the job ends | frees the card |
| `GET /api/gpus` | the Host tab | every card with live load/VRAM/temperature, its leases, and each job type's weight on it right now |
| `GET/PUT /api/policy` | the Host tab | the policy (validated on PUT) |
| `GET /api/live` | the Host tab | CPU per core, load, temperatures, every card — 2 s samples, 3 min of history |

**Policy** (`policy.json` in the `broker_data` volume): for each job type — `keying.corridorkey`, `keying.auto`,
`keying.rvm`, `forager`, and anything new that claims (it is added with defaults) — a VRAM floor, a default weight,
and per card a weight from 0 (never) to 10 plus optional **schedule windows** (days, from, to, weight; `TZ`, a
window may cross midnight) that replace the weight while they apply. A claim takes the free card with the
highest weight (ties: fewer leases, then lower utilisation, then more free VRAM); a card is free when its free VRAM
minus what fresh leases (< 3 min) have not allocated yet is at least the floor. Weights are preferences, not
reservations: if the preferred card is busy the job takes the best free one. Cards are addressed by UUID, so a new
GPU is used the moment nvidia-smi shows it (at the type's default weight).

The broker needs `nvidia-smi`: the compose file bind-mounts it and `libnvidia-ml.so.1` from `/opt/nvidia` in the
LXC (copied from the host's driver — same version as the kernel module) and the `/dev/nvidia*` nodes (one line per
card; add one when a GPU is added). Clients: the keying pipeline (`GPU_BROKER_URL` on its Settings page) and
forager (`GPU_BROKER_URL` in its `.env`) both fall back to their old local behaviour when the broker is unreachable.

```bash
cp .env.example .env            # upstream addresses, broker port, time zone
./fetch-fonts.sh                # once: vendors Atkinson Hyperlegible into site/fonts (needs internet)
docker compose up -d
```

Runs in LXC 600 `dashboard` on sisk-pm-01 at http://192.168.2.121/. No login — DCMP office LAN only.
