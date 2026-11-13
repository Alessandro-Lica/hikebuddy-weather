"""HikeBuddy weather backend: fetch the Open-Meteo forecast of every cell once, publish one bundle.

Run by GitHub Actions 5 times a day (.github/workflows/fetch.yml). The phones never call Open-Meteo:
they download site/latest.json and the bundle it names (GitHub Pages).

  cells.json         forecast points {"coarse" | "fine": [[cell id, lat, lon, elev], ...]} (written by
                     hikebuddy/pipeline/weather_cells.py from the app data)
  site/latest.json   {"v", "fetched", "start", "days", "file", "bytes", "cells", "calls"}
  site/<file>        the bundle (see encode())
  stats/requests.csv every request: time, run, level, locations, calls, HTTP status (429 = refused)
  stats/runs.csv     one row per run
  stats/daily.csv    per date (Europe/Rome): runs, requests, calls, max calls in any minute / hour,
                     refused; recomputed from requests.csv

Usage: python fetch.py [--run ID] [--limit N]           fetch (N: only the first N cells per level)
       python fetch.py --from-fixture forecast.json OUT  encode a saved forecast (app tests)
Standard library only.
"""
import argparse
import csv
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).parent
ROME = ZoneInfo("Europe/Rome")
CHUNK = 100  # locations per request
PAST_DAYS = 7  # fine cells: snow history of the 7 days before the hike
FORECAST_DAYS = 7
HOURS = (7, 21)  # rows kept for the window variables: 07:00-20:59 (ranking 09-18, detail page 07-20)
# Open-Meteo hourly variables. "feels like" (apparent_temperature) is not fetched: 10 variables count
# 1 call per location, 11 count 1.1.
VARS = {
    "coarse": ["precipitation", "precipitation_probability", "weather_code", "snowfall"],
    "fine": ["temperature_2m", "precipitation", "precipitation_probability", "weather_code", "cape",
             "snowfall", "snow_depth", "freezing_level_height", "wind_gusts_10m", "sunshine_duration"],
}
ALL_HOURS = {"fine": {"precipitation", "snowfall", "freezing_level_height"}}  # every hour: snow history
PAST = {"coarse": 0, "fine": PAST_DAYS}
BUDGET_MIN = 540  # calls per rolling minute (free tier: 600)
UA = {"User-Agent": "hikebuddy-weather/1 (github actions)"}


class LimitError(Exception):
    pass


def weight(level):
    return max(1.0, len(VARS[level]) / 10)


def url(level, cells):
    return ("https://api.open-meteo.com/v1/forecast"
            "?latitude=" + ",".join(c[1] for c in cells)
            + "&longitude=" + ",".join(c[2] for c in cells)
            + "&elevation=" + ",".join(c[3] for c in cells)
            + "&hourly=" + ",".join(VARS[level]) + f"&timezone=Europe%2FRome&forecast_days={FORECAST_DAYS}"
            + (f"&past_days={PAST[level]}" if PAST[level] else ""))


class Fetcher:
    def __init__(self, run):
        self.run = run
        self.log = []  # (utc datetime, level, locations, calls, status)

    def _wait_budget(self, calls):
        now = time.time()
        recent = [(t.timestamp(), c) for t, _, _, c, _ in self.log if now - t.timestamp() < 60]
        used = sum(c for _, c in recent)
        if used + calls <= BUDGET_MIN:
            return
        for t, c in sorted(recent):
            used -= c
            if used + calls <= BUDGET_MIN:
                time.sleep(max(0.0, t + 60.5 - now))
                return

    def get(self, level, cells):
        calls = round(len(cells) * weight(level), 1)
        u = url(level, cells)
        for attempt in range(6):
            self._wait_budget(calls)
            at = datetime.now(timezone.utc)
            try:
                with urllib.request.urlopen(urllib.request.Request(u, headers=UA), timeout=120) as r:
                    body = r.read()
                self.log.append((at, level, len(cells), calls, 200))
                js = json.loads(body)
                js = js if isinstance(js, list) else [js]
                if len(js) != len(cells):
                    raise RuntimeError(f"{len(js)} locations for {len(cells)}")
                return js
            except urllib.error.HTTPError as e:
                reason = e.read().decode("utf-8", "replace")[:300]
                self.log.append((at, level, len(cells), calls, e.code))
                print(f"  {level}: HTTP {e.code} {reason}", flush=True)
                if e.code == 429 and ("hourly" in reason.lower() or "daily" in reason.lower()):
                    raise LimitError(reason)
                if e.code not in (429, 500, 502, 503, 504):
                    raise
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                self.log.append((at, level, len(cells), calls, 0))
                print(f"  {level}: {e}", flush=True)
            time.sleep(30 * (attempt + 1))
        raise RuntimeError(f"{level}: giving up after 6 attempts")


# ---------------------------------------------------------------- bundle
def scale_of(values):
    """Smallest power of 10 (1 .. 10^4) that makes every value an integer (lossless)."""
    for s in (1, 10, 100, 1000, 10000):
        if all(v is None or abs(v * s - round(v * s)) < 1e-6 for v in values):
            return s
    raise ValueError("more than 4 decimals")


def encode(fetched, cells_hourly, elev, start=None):
    """cells_hourly: {level: {cell id: hourly dict from Open-Meteo}}, elev: {level: {cell id: m}} ->
    {"v": 1, "fetched", "start", "days", "hours": [from, to),
     level: {"time": [...], "cells": [ids], "elev": [m], "vars": {name: {"rows": "all" | "win", "scale", "data"}}}}.
    elev = the point's elevation asked for (the app shifts temperatures from it to the hike's top).
    start: first forecast day (default: from the time axis; saved fixtures may be trimmed).
    data = cell-major integers (value * scale, null kept); "win" rows = days start .. start+6, hours
    HOURS; "all" = every row of "time". Every cell of a level has the same "time"."""
    out = {"v": 1, "fetched": fetched, "days": FORECAST_DAYS, "hours": list(HOURS)}
    for level, by_cell in cells_hourly.items():
        ids = sorted(by_cell)
        time_ = by_cell[ids[0]]["time"]
        for c in ids:
            if by_cell[c]["time"] != time_:
                raise RuntimeError(f"{level} {c}: different time axis")
        start = start or time_[PAST[level] * 24][:10]
        out.setdefault("start", start)
        win = [i for i, t in enumerate(time_) if t[:10] >= start and HOURS[0] <= int(t[11:13]) < HOURS[1]]
        vs = {}
        for v in VARS[level]:
            rows = "all" if v in ALL_HOURS.get(level, ()) else "win"
            idx = range(len(time_)) if rows == "all" else win
            vals = [by_cell[c][v][i] for c in ids for i in idx]
            s = scale_of(vals)
            vs[v] = {"rows": rows, "scale": s, "data": [None if x is None else round(x * s) for x in vals]}
        out[level] = {"time": time_, "cells": ids, "elev": [elev[level][c] for c in ids], "vars": vs}
    return out


def write_site(bundle, out_dir: Path, calls):
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.fromisoformat(bundle["fetched"]).astimezone(timezone.utc).strftime("%Y%m%dT%H%MZ")
    name = f"forecast-{stamp}.json"
    body = json.dumps(bundle, separators=(",", ":")).encode()
    (out_dir / name).write_bytes(body)
    latest = {"v": 1, "fetched": bundle["fetched"], "start": bundle["start"], "days": bundle["days"],
              "file": name, "bytes": len(body),
              "cells": {lv: len(bundle[lv]["cells"]) for lv in ("coarse", "fine") if lv in bundle},
              "calls": calls}
    (out_dir / "latest.json").write_text(json.dumps(latest, indent=1) + "\n")
    (out_dir / "index.html").write_text(
        "<!doctype html><meta charset=utf-8><title>hikebuddy-weather</title>"
        f"<p>Forecast bundle for the HikeBuddy app. Latest: <a href=latest.json>latest.json</a> "
        f"(fetched {bundle['fetched']}). Weather data by <a href=https://open-meteo.com/>Open-Meteo.com</a> "
        "(CC BY 4.0).</p>\n")
    return latest


# ---------------------------------------------------------------- stats
REQ_HEAD = ["time_utc", "run", "level", "locations", "calls", "status"]
RUN_HEAD = ["run", "start_utc", "end_utc", "status", "requests", "calls", "refused", "bundle_bytes", "note"]
DAY_HEAD = ["date", "runs", "requests", "calls", "max_calls_1min", "max_calls_1h", "refused"]


def append_csv(path: Path, head, rows):
    new = not path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(head)
        w.writerows(rows)


def rolling_max(times_calls, seconds):
    best, j, s = 0.0, 0, 0.0
    for i, (t, c) in enumerate(times_calls):
        s += c
        while t - times_calls[j][0] >= seconds:
            s -= times_calls[j][1]
            j += 1
        best = max(best, s)
    return best


def write_daily(stats: Path):
    by_day = {}
    with (stats / "requests.csv").open(newline="") as f:
        for r in csv.DictReader(f):
            t = datetime.fromisoformat(r["time_utc"])
            by_day.setdefault(t.astimezone(ROME).date().isoformat(), []).append((t.timestamp(), float(r["calls"]), r))
    with (stats / "daily.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(DAY_HEAD)
        for d in sorted(by_day):
            rows = sorted(by_day[d], key=lambda x: x[0])
            tc = [(t, c) for t, c, _ in rows]
            w.writerow([d, len({r["run"] for _, _, r in rows}), len(rows), round(sum(c for _, c in tc), 1),
                        round(rolling_max(tc, 60), 1), round(rolling_max(tc, 3600), 1),
                        sum(r["status"] == "429" for _, _, r in rows)])


# ---------------------------------------------------------------- main
def main_fetch(run, limit):
    cells = json.loads((ROOT / "cells.json").read_text())
    f = Fetcher(run)
    t0 = datetime.now(timezone.utc)
    status, note, latest = "ok", "", None
    try:
        got = {}
        for level in ("coarse", "fine"):
            pts = cells[level][:limit] if limit else cells[level]
            got[level] = {}
            for i in range(0, len(pts), CHUNK):
                chunk = pts[i:i + CHUNK]
                for c, j in zip(chunk, f.get(level, chunk)):
                    got[level][c[0]] = j["hourly"]
                print(f"{level}: {len(got[level])}/{len(pts)}", flush=True)
        fetched = datetime.now(ROME).isoformat(timespec="seconds")
        elev = {lv: {c[0]: float(c[3]) for c in cells[lv]} for lv in cells}
        latest = write_site(encode(fetched, got, elev), ROOT / "site", round(sum(x[3] for x in f.log), 1))
        print(f"bundle {latest['file']}: {latest['bytes'] / 1e6:.2f} MB, {latest['calls']} calls")
    except LimitError as e:
        status, note = "limit", str(e)[:200]
    except Exception as e:  # noqa: BLE001 - recorded, then the run fails
        status, note = "error", f"{type(e).__name__}: {e}"[:200]
    stats = ROOT / "stats"
    append_csv(stats / "requests.csv", REQ_HEAD,
               [[t.isoformat(timespec="seconds"), run, lv, n, c, s] for t, lv, n, c, s in f.log])
    append_csv(stats / "runs.csv", RUN_HEAD,
               [[run, t0.isoformat(timespec="seconds"), datetime.now(timezone.utc).isoformat(timespec="seconds"),
                 status, len(f.log), round(sum(x[3] for x in f.log), 1), sum(x[4] == 429 for x in f.log),
                 latest["bytes"] if latest else "", note]])
    if (stats / "requests.csv").exists():
        write_daily(stats)
    if status != "ok":
        print(f"FAILED ({status}): {note}", file=sys.stderr)
        sys.exit(1)


def main_fixture(src, out, start=None):
    """Golden forecast.json ({level: {cell: {lat, lon, elev, hourly}}}) -> bundle, as if fetched."""
    js = json.loads(Path(src).read_text())
    got = {lv: {c: v["hourly"] for c, v in js[lv].items()} for lv in ("coarse", "fine") if js.get(lv)}
    elev = {lv: {c: v["elev"] for c, v in js[lv].items()} for lv in got}
    for lv in got:  # the fixture may hold more variables (apparent_temperature)
        for h in got[lv].values():
            for v in VARS[lv]:
                h.setdefault(v, [None] * len(h["time"]))
    first = start or next(iter(got["coarse"].values()))["time"][0][:10]
    Path(out).write_text(json.dumps(encode(f"{first}T06:00:00+01:00", got, elev, first), separators=(",", ":")))
    print(f"{out} written")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", default="local")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--from-fixture", nargs="+", metavar="FORECAST_JSON OUT [START_DAY]")
    a = ap.parse_args()
    if a.from_fixture:
        main_fixture(*a.from_fixture)
    else:
        main_fetch(a.run, a.limit)
