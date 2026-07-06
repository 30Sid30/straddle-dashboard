#!/usr/bin/env python3
"""
download_spot.py
================
Incremental download of 1-min spot candles for NIFTY and SENSEX.
On each run, reads the last timestamp in the existing CSV and only
fetches newer data — avoids re-downloading from Jan 2024 every time.

Output:
  Data/NIFTY_SPOT.csv   (NSE-NIFTY, SEGMENT_CASH)
  Data/SENSEX_SPOT.csv  (BSE-SENSEX, SEGMENT_CASH)
"""

import csv
import os
import time
import threading
from collections import deque
from datetime import date, datetime, timedelta
from pathlib import Path

import pyotp
from growwapi import GrowwAPI

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CREDENTIALS  (same JWT / TOTP as download_data.py)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
API_KEY     = os.environ["GROWW_API_KEY"]
TOTP_SECRET = os.environ["GROWW_TOTP_SECRET"]

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR   = BASE_DIR / "Data"
DATA_START = date(2024, 1, 1)
CHUNK_DAYS = 30

CSV_HEADERS = ["timestamp", "open", "high", "low", "close", "volume", "open_interest"]

SPOT_CONFIGS = [
    {
        "name":     "NIFTY",
        "exchange": GrowwAPI.EXCHANGE_NSE,
        "symbol":   "NSE-NIFTY",
        "output":   DATA_DIR / "NIFTY_SPOT.csv",
    },
    {
        "name":     "SENSEX",
        "exchange": GrowwAPI.EXCHANGE_BSE,
        "symbol":   "BSE-SENSEX",
        "output":   DATA_DIR / "SENSEX_SPOT.csv",
    },
]

# ── Thread-safe rate limiter ───────────────────────────────────────────────────
class RateLimiter:
    def __init__(self, per_second: int = 5, per_minute: int = 300):
        self._ps  = per_second
        self._pm  = per_minute
        self._win: deque = deque()
        self._lk  = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lk:
                now = time.monotonic()
                while self._win and self._win[0] < now - 60:
                    self._win.popleft()
                in_sec = sum(1 for t in self._win if t >= now - 1.0)
                if len(self._win) >= self._pm:
                    sleep = 60 - (now - self._win[0]) + 0.05
                elif in_sec >= self._ps:
                    oldest = next(t for t in self._win if t >= now - 1.0)
                    sleep = 1.0 - (now - oldest) + 0.05
                else:
                    self._win.append(time.monotonic())
                    return
            time.sleep(sleep)

_rl   = RateLimiter()
_auth = {"groww": None, "at": 0.0}


def get_groww() -> GrowwAPI:
    now = time.time()
    if _auth["groww"] is None or (now - _auth["at"]) >= 6 * 3600:
        print("Authenticating...")
        token = GrowwAPI.get_access_token(
            api_key=API_KEY, totp=pyotp.TOTP(TOTP_SECRET).now()
        )
        _auth["groww"] = GrowwAPI(token)
        _auth["at"]    = now
        print("Auth OK.")
    return _auth["groww"]


# ── Helpers ───────────────────────────────────────────────────────────────────
def last_row_date(filepath: Path) -> date | None:
    """Return the date of the last data row in an existing spot CSV."""
    if not filepath.exists():
        return None
    try:
        with open(filepath) as f:
            lines = f.readlines()
        for line in reversed(lines):
            s = line.strip()
            if s and not s.startswith("timestamp"):
                return datetime.fromisoformat(s.split(",")[0]).date()
    except Exception:
        pass
    return None


def build_chunks(start: date, end: date) -> list[tuple[date, date]]:
    chunks, cur = [], start
    while cur < end:
        chunk_end = min(cur + timedelta(days=CHUNK_DAYS - 1), end)
        chunks.append((cur, chunk_end))
        cur = chunk_end + timedelta(days=1)
    return chunks


# ── Per-symbol download ────────────────────────────────────────────────────────
def download_spot(cfg: dict) -> None:
    name   = cfg["name"]
    output = cfg["output"]
    output.parent.mkdir(parents=True, exist_ok=True)

    last_date  = last_row_date(output)
    start_date = (last_date + timedelta(days=1)) if last_date else DATA_START
    today      = date.today()

    if start_date > today:
        print(f"[{name}] Already up to date (last row: {last_date}).")
        return

    chunks = build_chunks(start_date, today)
    print(f"[{name}] {len(chunks)} chunk(s): {start_date} → {today}")

    new_rows: list = []
    seen:     set  = set()

    for i, (s, e) in enumerate(chunks):
        s_str = f"{s} 09:15:00"
        e_str = f"{e} 15:30:00"
        print(f"  [{name}] chunk {i+1}/{len(chunks)}: {s_str} → {e_str}", end="\r")
        _rl.acquire()
        try:
            resp = get_groww().get_historical_candles(
                exchange=cfg["exchange"],
                segment=GrowwAPI.SEGMENT_CASH,
                groww_symbol=cfg["symbol"],
                start_time=s_str,
                end_time=e_str,
                candle_interval=GrowwAPI.CANDLE_INTERVAL_MIN_1,
                timeout=10,
            )
            for row in resp.get("candles", []):
                if row[0] not in seen:
                    seen.add(row[0])
                    new_rows.append(row)
        except Exception as exc:
            print(f"\n  [{name}] chunk {s}→{e} error: {exc}")
            time.sleep(3)

    if not new_rows:
        print(f"\n[{name}] No new data.")
        return

    new_rows.sort(key=lambda r: r[0])

    # Append if file exists with prior data; otherwise write fresh with header
    mode = "a" if (output.exists() and last_date) else "w"
    with open(output, mode, newline="") as f:
        w = csv.writer(f)
        if mode == "w":
            w.writerow(CSV_HEADERS)
        w.writerows(new_rows)

    print(f"\n[{name}] +{len(new_rows)} rows → {output.name}")


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    get_groww()
    for cfg in SPOT_CONFIGS:
        download_spot(cfg)
    print("Spot download complete.")


if __name__ == "__main__":
    main()
