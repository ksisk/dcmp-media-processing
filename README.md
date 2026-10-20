# DCMP Media portal

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
per core, every GPU's load and VRAM, temperatures — read from the keying app's `/api/live`) and a GPUs table with
the jobs from both apps on each card (forager's current encode is placed on `FORAGER_GPU`, the card it has).

```bash
cp .env.example .env            # upstream addresses
./fetch-fonts.sh                # once: vendors Atkinson Hyperlegible into site/fonts (needs internet)
docker compose up -d
```

Runs in LXC 600 `dashboard` on sisk-pm-01 at http://192.168.25.30/. No login — LAN/VPN only.
