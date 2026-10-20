#!/usr/bin/env python3
"""DCMP Media host — GPU broker.

One service on the host container decides which card every GPU job runs on, for every app (keying pipeline,
Media Forager, later the Windows workers). Apps ask for a card (`POST /api/claim`), keep the lease alive while
the job runs (`POST /api/heartbeat`) and give it back (`POST /api/release`). The choice follows the policy in
policy.json — a weight per job type per card, optionally different inside scheduled time windows, plus a
free-VRAM floor — and the live state of the cards from nvidia-smi.

Weights are preferences, not reservations: a job takes the best card that is free enough *now*; it only waits
when no card with weight > 0 has the VRAM. Weight 0 means "never on this card" (for that job type, at that time).

Also serves the host's live monitor (`GET /api/live`: CPU per core, load, temperatures, every card) so the
portal's Host tab reads the host from one place.

Stdlib only. State lives in DATA_DIR (policy.json, leases.json) so a restart loses nothing.
"""
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
import uuid as uuidlib
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

PORT = int(os.environ.get("BROKER_PORT", "8090"))
DATA = Path(os.environ.get("DATA_DIR", "/data"))
TZ_NAME = os.environ.get("TZ", "America/Los_Angeles")
LEASE_TTL = int(os.environ.get("LEASE_TTL", "600"))          # seconds without a heartbeat before a lease is dropped
YOUNG = int(os.environ.get("LEASE_RESERVE_SECONDS", "180"))  # a fresh lease reserves its VRAM floor this long (the job has not allocated yet)
LIVE_SECONDS = 2
LIVE_KEEP = 90

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger("broker")

# ----------------------------------------------------------------------------- GPUs (same shape as keying's gpus.py)
FIELDS = "index,uuid,name,memory.total,memory.free,memory.used,utilization.gpu,temperature.gpu,power.draw"


def short_name(name):
    n = (name or "").replace("NVIDIA", "").replace("GeForce", "").replace("Corporation", "").strip()
    return " ".join(n.split()) or (name or "GPU")


def list_gpus(timeout=10):
    """Every card nvidia-smi can see, or None when nvidia-smi is missing or failing."""
    try:
        out = subprocess.run(["nvidia-smi", f"--query-gpu={FIELDS}", "--format=csv,noheader,nounits"],
                             check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=timeout).stdout
    except Exception:
        return None
    gpus = []
    for ln in out.strip().splitlines():
        parts = [p.strip() for p in ln.split(",")]
        if len(parts) < 7:
            continue

        def num(s, cast=float):
            try:
                return cast(s)
            except ValueError:
                return None
        gpus.append({"index": num(parts[0], int), "uuid": parts[1], "name": parts[2], "short": short_name(parts[2]),
                     "mem_total": num(parts[3], int), "mem_free": num(parts[4], int), "mem_used": num(parts[5], int),
                     "util": num(parts[6], int), "temp": num(parts[7]) if len(parts) > 7 else None,
                     "power": num(parts[8]) if len(parts) > 8 else None})
    return gpus


# ----------------------------------------------------------------------------- policy
# policy.json:
# {
#   "job_types": {
#     "keying.corridorkey": {"label": "Keying · CorridorKey", "min_free_mb": 6000, "default_weight": 5,
#                            "gpus": {"GPU-xxxx": {"weight": 10, "schedule": [{"days": [0,1,2,3,4], "from": "19:00", "to": "07:00", "weight": 3}]}}},
#     ...
#   }
# }
# Effective weight of a (type, card) now = the first schedule entry whose window contains now, else the cell's
# weight, else the type's default_weight (so a brand-new card is usable the moment it appears). Days: 0 = Monday.
DEFAULT_TYPES = {
    "keying.corridorkey": {"label": "Keying · CorridorKey", "min_free_mb": 6000, "default_weight": 5, "gpus": {}},
    "keying.auto":        {"label": "Keying · Auto",        "min_free_mb": 4000, "default_weight": 5, "gpus": {}},
    "keying.rvm":         {"label": "Keying · Fast (RVM)",  "min_free_mb": 3000, "default_weight": 5, "gpus": {}},
    "forager":            {"label": "Media Forager",        "min_free_mb": 2000, "default_weight": 5, "gpus": {}},
}
_policy = {"job_types": {}}
_leases = {}                                  # id -> lease
_lock = threading.RLock()
_tz = ZoneInfo(TZ_NAME)


def _load():
    global _policy, _leases
    DATA.mkdir(parents=True, exist_ok=True)
    try:
        _policy = json.loads((DATA / "policy.json").read_text())
    except (OSError, ValueError):
        _policy = {"job_types": {}}
    for k, v in DEFAULT_TYPES.items():          # new job types appear with defaults; existing ones are kept as saved
        _policy.setdefault("job_types", {}).setdefault(k, json.loads(json.dumps(v)))
    try:
        _leases = {l["id"]: l for l in json.loads((DATA / "leases.json").read_text())}
    except (OSError, ValueError):
        _leases = {}


def _save_policy():
    tmp = DATA / "policy.tmp"
    tmp.write_text(json.dumps(_policy, indent=1))
    tmp.replace(DATA / "policy.json")


def _save_leases():
    tmp = DATA / "leases.tmp"
    tmp.write_text(json.dumps(list(_leases.values()), indent=1))
    tmp.replace(DATA / "leases.json")


_HHMM = re.compile(r"^([01]?\d|2[0-3]):([0-5]\d)$")


def _minutes(hhmm):
    m = _HHMM.match(hhmm or "")
    if not m:
        raise ValueError(f"bad time {hhmm!r} (HH:MM)")
    return int(m.group(1)) * 60 + int(m.group(2))


def validate_policy(p):
    """Raise ValueError on anything the editor could have produced wrong; return a cleaned copy."""
    if not isinstance(p, dict) or not isinstance(p.get("job_types"), dict):
        raise ValueError("policy needs a job_types object")
    out = {"job_types": {}}
    for name, t in p["job_types"].items():
        if not re.match(r"^[a-z][a-z0-9_.-]{0,40}$", name):
            raise ValueError(f"bad job type name {name!r}")
        if not isinstance(t, dict):
            raise ValueError(f"{name}: not an object")
        c = {"label": str(t.get("label") or name)[:60],
             "min_free_mb": max(0, int(t.get("min_free_mb") or 0)),
             "default_weight": min(10, max(0, int(t.get("default_weight", 5)))),
             "gpus": {}}
        for uid, cell in (t.get("gpus") or {}).items():
            if not isinstance(cell, dict):
                raise ValueError(f"{name}/{uid}: not an object")
            cc = {"weight": min(10, max(0, int(cell.get("weight", c["default_weight"])))), "schedule": []}
            for s in cell.get("schedule") or []:
                days = sorted({int(d) for d in (s.get("days") or [0, 1, 2, 3, 4, 5, 6])})
                if any(d < 0 or d > 6 for d in days):
                    raise ValueError(f"{name}/{uid}: days must be 0-6 (Monday = 0)")
                _minutes(s.get("from")), _minutes(s.get("to"))
                cc["schedule"].append({"days": days, "from": s["from"], "to": s["to"],
                                       "weight": min(10, max(0, int(s.get("weight", 0))))})
            c["gpus"][uid] = cc
        out["job_types"][name] = c
    return out


def in_window(s, now):
    """Is local time `now` inside schedule entry s? Windows may cross midnight (19:00-07:00): the day is the day the
    window starts."""
    a, b = _minutes(s["from"]), _minutes(s["to"])
    m = now.hour * 60 + now.minute
    wd = now.weekday()
    if a == b:
        return wd in s["days"]
    if a < b:
        return wd in s["days"] and a <= m < b
    # crosses midnight: tonight's part belongs to today, the early-morning part to yesterday's window
    if m >= a:
        return wd in s["days"]
    if m < b:
        return ((wd - 1) % 7) in s["days"]
    return False


def effective_weight(jt, card_uuid, now=None):
    """(weight, source) for a job type on a card right now. source: 'schedule', 'card' or 'default'."""
    now = now or datetime.now(_tz)
    t = _policy["job_types"].get(jt)
    if t is None:
        return 0, "unknown type"
    cell = t.get("gpus", {}).get(card_uuid)
    if cell:
        for s in cell.get("schedule", []):
            if in_window(s, now):
                return s["weight"], "schedule"
        return cell["weight"], "card"
    return t.get("default_weight", 5), "default"


# ----------------------------------------------------------------------------- leases
def _expire(now=None):
    now = now or time.time()
    dead = [l for l in _leases.values() if now - l["seen"] > LEASE_TTL]
    for l in dead:
        log.warning("lease %s (%s %s on %s) expired: no heartbeat for %ds", l["id"], l["app"], l["job"], l["short"], now - l["seen"])
        _leases.pop(l["id"], None)
    if dead:
        _save_leases()


def _reserved(card_uuid, now):
    """VRAM (MB) young leases on this card are expected to take but have not allocated yet."""
    return sum(l.get("min_free_mb", 0) for l in _leases.values()
               if l["uuid"] == card_uuid and now - l["started"] < YOUNG)


def claim(app, job, jt, min_free_mb=None):
    """Pick a card for (app, job) of job type jt. Returns (lease, None) or (None, reason-to-wait)."""
    with _lock:
        now = time.time()
        _expire(now)
        for l in _leases.values():                       # same job asking again (restart/resume) keeps its card
            if l["app"] == app and l["job"] == job:
                l["seen"] = now
                return l, None
        cards = list_gpus()
        if cards is None:
            return None, "nvidia-smi not available on the host"
        if not cards:
            return None, "no GPUs"
        t = _policy["job_types"].get(jt)
        if t is None:                                    # unknown type: register it with defaults so it shows in the editor
            t = _policy["job_types"][jt] = {"label": jt, "min_free_mb": 2000, "default_weight": 5, "gpus": {}}
            _save_policy()
        floor = int(min_free_mb) if min_free_mb is not None else t["min_free_mb"]
        ldt = datetime.now(_tz)
        ok, why = [], []
        for c in cards:
            w, src = effective_weight(jt, c["uuid"], ldt)
            if w <= 0:
                why.append(f"{c['short']}: weight 0 ({src})")
                continue
            free = (c["mem_free"] or 0) - _reserved(c["uuid"], now)
            if free < floor:
                why.append(f"{c['short']}: {int(free)} MB free < {floor} MB")
                continue
            held = sum(1 for l in _leases.values() if l["uuid"] == c["uuid"])
            ok.append((w, -held, -(c["util"] or 0), free, c))
        if not ok:
            return None, "; ".join(why) or "no card qualifies"
        ok.sort(key=lambda r: (r[0], r[1], r[2], r[3]), reverse=True)
        w, _, _, free, c = ok[0]
        lease = {"id": uuidlib.uuid4().hex[:12], "app": app, "job": job, "type": jt, "uuid": c["uuid"], "index": c["index"],
                 "short": c["short"], "name": c["name"], "weight": w, "min_free_mb": floor, "started": now, "seen": now}
        _leases[lease["id"]] = lease
        _save_leases()
        log.info("claim %s %s (%s) -> %s [index %s] weight %s, %d MB free", app, job, jt, c["short"], c["index"], w, free)
        return lease, None


def heartbeat(lease_id):
    with _lock:
        l = _leases.get(lease_id)
        if not l:
            return False
        l["seen"] = time.time()
        return True


def release(lease_id):
    with _lock:
        l = _leases.pop(lease_id, None)
        if l:
            _save_leases()
            log.info("release %s %s (%s) from %s after %.0f s", l["app"], l["job"], l["type"], l["short"], time.time() - l["started"])
        return bool(l)


def gpus_view():
    """Cards + who holds them + the weight every job type has on each card right now."""
    with _lock:
        _expire()
        cards = list_gpus()
        ldt = datetime.now(_tz)
        if cards is None:
            return {"available": False, "gpus": [], "leases": [], "now": time.time()}
        for c in cards:
            c["leases"] = [l for l in _leases.values() if l["uuid"] == c["uuid"]]
            c["weights"] = {jt: effective_weight(jt, c["uuid"], ldt)[0] for jt in _policy["job_types"]}
        return {"available": True, "gpus": cards, "leases": sorted(_leases.values(), key=lambda l: l["started"]),
                "now": time.time(), "local_time": ldt.strftime("%a %H:%M"), "tz": TZ_NAME}


# ----------------------------------------------------------------------------- live monitor (host CPU/temps + cards)
_live = {"hist": [], "prev": None, "latest": None, "lock": threading.Lock()}


def _read_proc_stat():
    out = {}
    with open("/proc/stat") as f:
        for ln in f:
            if not ln.startswith("cpu"):
                break
            parts = ln.split()
            vals = [int(x) for x in parts[1:]]
            idle = vals[3] + (vals[4] if len(vals) > 4 else 0)
            out[parts[0]] = (idle, sum(vals))
    return out


def _cpu_pcts(prev, cur):
    def pct(k):
        if k not in prev or k not in cur:
            return None
        di, dt = cur[k][0] - prev[k][0], cur[k][1] - prev[k][1]
        return round(100 * (1 - di / dt), 1) if dt > 0 else 0.0
    cores = sorted((k for k in cur if k != "cpu"), key=lambda k: int(k[3:]))
    return pct("cpu"), [pct(k) for k in cores]


def _read_temps():
    out = []
    try:
        for hw in sorted(Path("/sys/class/hwmon").glob("hwmon*")):
            try:
                chip = (hw / "name").read_text().strip()
            except OSError:
                continue
            for inp in sorted(hw.glob("temp*_input")):
                try:
                    t = int(inp.read_text().strip()) / 1000
                except (OSError, ValueError):
                    continue
                lab = inp.with_name(inp.name.replace("_input", "_label"))
                try:
                    label = lab.read_text().strip()
                except OSError:
                    label = inp.name.replace("_input", "")
                if -20 < t < 150:
                    out.append({"chip": chip, "label": label, "temp": round(t, 1)})
        if not out:
            for tz in sorted(Path("/sys/class/thermal").glob("thermal_zone*")):
                try:
                    out.append({"chip": "thermal", "label": (tz / "type").read_text().strip(),
                                "temp": round(int((tz / "temp").read_text().strip()) / 1000, 1)})
                except (OSError, ValueError):
                    pass
    except OSError:
        pass
    return out


CPU_TEMP_CHIPS = ("coretemp", "k10temp", "zenpower", "cpu_thermal", "x86_pkg_temp")


def _cpu_temp(temps):
    cpu = [t for t in temps if t["chip"] in CPU_TEMP_CHIPS or t["label"] in CPU_TEMP_CHIPS]
    pk = [t for t in cpu if any(k in t["label"].lower() for k in ("package", "tctl", "tdie", "x86_pkg"))]
    pool = pk or cpu
    return max((t["temp"] for t in pool), default=None)


def live_loop():
    while True:
        try:
            cur = _read_proc_stat()
            prev = _live["prev"]
            _live["prev"] = cur
            if prev is None:
                time.sleep(LIVE_SECONDS)
                continue
            cpu, cores = _cpu_pcts(prev, cur)
            try:
                load1 = os.getloadavg()[0]
            except OSError:
                load1 = None
            cards = list_gpus(timeout=5) or []
            temps = _read_temps()
            sample = {"t": round(time.time(), 1), "cpu": cpu, "cores": cores, "load1": load1,
                      "cpu_temp": _cpu_temp(temps), "temps": temps,
                      "gpus": [{"index": g["index"], "short": g["short"], "uuid": g["uuid"], "util": g["util"],
                                "mem_used": g["mem_used"], "mem_total": g["mem_total"], "temp": g["temp"], "power": g["power"]}
                               for g in cards]}
            with _live["lock"]:
                _live["latest"] = sample
                _live["hist"].append({"t": sample["t"], "cpu": cpu, "cpu_temp": sample["cpu_temp"], "gpu": [g["util"] for g in sample["gpus"]],
                                      "gtemp": [g["temp"] for g in sample["gpus"]],
                                      "vram": [round(100 * (g["mem_used"] or 0) / g["mem_total"], 1) if g["mem_total"] else None for g in sample["gpus"]]})
                del _live["hist"][:-LIVE_KEEP]
        except Exception:
            log.exception("live sampler")
        time.sleep(LIVE_SECONDS)


def live_json():
    with _live["lock"]:
        return {"now": time.time(), "interval": LIVE_SECONDS, "latest": _live["latest"], "hist": list(_live["hist"])}


# ----------------------------------------------------------------------------- HTTP
class H(BaseHTTPRequestHandler):
    server_version = "dcmp-gpu-broker/1"

    def log_message(self, fmt, *a):            # quiet: the polling is 2 s
        if self.command != "GET":
            log.info("%s %s %s", self.address_string(), self.command, self.path)

    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b""
        try:
            return json.loads(raw or b"{}")
        except ValueError:
            raise ValueError("body is not JSON")

    def do_GET(self):
        p = urlparse(self.path).path.rstrip("/")
        if p in ("/api/health", "/health"):
            return self._json({"ok": True, "leases": len(_leases), "tz": TZ_NAME})
        if p == "/api/gpus":
            return self._json(gpus_view())
        if p == "/api/live":
            return self._json(live_json())
        if p == "/api/policy":
            with _lock:
                return self._json({"policy": _policy, "tz": TZ_NAME, "local_time": datetime.now(_tz).strftime("%a %H:%M")})
        self._json({"error": "not found"}, 404)

    def do_PUT(self):
        p = urlparse(self.path).path.rstrip("/")
        if p == "/api/policy":
            try:
                new = validate_policy(self._body())
            except (ValueError, TypeError) as e:
                return self._json({"error": str(e)}, 400)
            with _lock:
                _policy.clear()
                _policy.update(new)
                _save_policy()
            log.info("policy updated: %s", ", ".join(f"{k}: {[(u[-4:], c['weight'], len(c['schedule'])) for u, c in v['gpus'].items()]}" for k, v in new["job_types"].items()))
            return self._json({"ok": True, "policy": _policy})
        self._json({"error": "not found"}, 404)

    def do_POST(self):
        p = urlparse(self.path).path.rstrip("/")
        try:
            b = self._body()
        except ValueError as e:
            return self._json({"error": str(e)}, 400)
        if p == "/api/claim":
            app, job, jt = str(b.get("app") or ""), str(b.get("job") or ""), str(b.get("type") or "")
            if not (app and job and jt):
                return self._json({"error": "app, job and type are required"}, 400)
            lease, why = claim(app, job, jt, b.get("min_free_mb"))
            if lease is None:
                return self._json({"wait": True, "reason": why})
            return self._json({"wait": False, "lease": lease["id"], "uuid": lease["uuid"], "index": lease["index"],
                               "short": lease["short"], "name": lease["name"], "weight": lease["weight"], "ttl": LEASE_TTL})
        if p == "/api/heartbeat":
            return self._json({"ok": heartbeat(str(b.get("lease") or ""))})
        if p == "/api/release":
            return self._json({"ok": release(str(b.get("lease") or ""))})
        self._json({"error": "not found"}, 404)


def main():
    _load()
    log.info("GPU broker on :%d, data %s, tz %s, %d lease(s) restored, cards: %s", PORT, DATA, TZ_NAME, len(_leases),
             ", ".join(f"{g['index']}: {g['short']}" for g in (list_gpus() or [])) or "none")
    threading.Thread(target=live_loop, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), H)
    srv.daemon_threads = True
    srv.serve_forever()


if __name__ == "__main__":
    main()
