#!/usr/bin/env python3
"""
download_0dte.py
================
Downloads 0DTE (expiry-day-only) options data for the last N_EXPIRIES
of NIFTY and SENSEX. Only ATM ± ATM_STRIKE_RANGE strikes are fetched.

Expiry calendar logic:
  - Dates are fetched from the Groww API and cached in Data/expiry_cache.json.
  - Cache is refreshed only when the next known upcoming expiry has passed
    (a new cycle began), or when the cache is older than 7 days.
  - This avoids redundant API calls that would return no new data.

Download guard:
  - Data is only downloaded for an expiry that is fully past, OR for today's
    expiry after 9 PM (markets settled, full-day data available).

0DTE optimisation:
  - Each contract is fetched for the expiry day only (09:15–15:30), not
    its full lifetime. This cuts API calls by ~99% vs the bulk downloader.

Parallelism:
  - Contract downloads run in a ThreadPoolExecutor. All workers share a
    single thread-safe RateLimiter to stay within Groww API limits.
"""

import csv
import json
import os
import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pyotp
from growwapi import GrowwAPI

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CREDENTIALS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
API_KEY     = os.environ["GROWW_API_KEY"]
TOTP_SECRET = os.environ["GROWW_TOTP_SECRET"]

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

N_EXPIRIES            = 90    # past expiries to download per index
ATM_STRIKE_RANGE      = 15    # strikes on each side of ATM
MAX_WORKERS           = 8     # parallel download threads
MARKET_CLOSE_HOUR     = 21    # download today's expiry only after 9 PM
TOKEN_REFRESH_SECS    = 6 * 3600
MAX_RETRIES           = 5
RETRY_BASE_WAIT       = 1.5   # seconds; doubles on each retry
EXPIRY_CACHE_FILE     = BASE_DIR / "Data" / "expiry_cache.json"
CACHE_MAX_AGE_DAYS    = 7     # hard refresh even if next_expiry hasn't passed

NIFTY_STRIKE_INTERVAL  = 50
SENSEX_STRIKE_INTERVAL = 100

CSV_HEADERS = ["timestamp", "open", "high", "low", "close", "volume", "open_interest"]

INDICES = [
    {
        "name":      "NIFTY",
        "exchange":  GrowwAPI.EXCHANGE_NSE,
        "spot_file": BASE_DIR / "Data" / "NIFTY_SPOT.csv",
        "interval":  NIFTY_STRIKE_INTERVAL,
    },
    {
        "name":      "SENSEX",
        "exchange":  GrowwAPI.EXCHANGE_BSE,
        "spot_file": BASE_DIR / "Data" / "SENSEX_SPOT.csv",
        "interval":  SENSEX_STRIKE_INTERVAL,
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
_alck = threading.Lock()


def get_groww() -> GrowwAPI:
    with _alck:
        now = time.time()
        if _auth["groww"] is None or (now - _auth["at"]) >= TOKEN_REFRESH_SECS:
            totp  = pyotp.TOTP(TOTP_SECRET).now()
            token = GrowwAPI.get_access_token(api_key=API_KEY, totp=totp)
            _auth["groww"] = GrowwAPI(token)
            _auth["at"]    = now
            print("Auth OK.")
    return _auth["groww"]


def call_api(fn, *args, **kwargs):
    kwargs.setdefault("timeout", 10)
    wait = RETRY_BASE_WAIT
    for attempt in range(1, MAX_RETRIES + 1):
        _rl.acquire()
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            if attempt == MAX_RETRIES:
                raise
            err = str(exc)
            if "auth" in err.lower() or "401" in err:
                with _alck:
                    _auth["at"] = 0.0
            time.sleep(wait)
            wait *= 2


# ── Expiry cache ──────────────────────────────────────────────────────────────
def _load_cache() -> dict:
    if EXPIRY_CACHE_FILE.exists():
        try:
            with open(EXPIRY_CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_cache(cache: dict) -> None:
    EXPIRY_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(EXPIRY_CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


def _cache_needs_refresh(entry: dict) -> bool:
    """
    True when:
      - Entry is missing (first run for this index).
      - The next known upcoming expiry has now passed → new cycle started.
      - Cache is older than CACHE_MAX_AGE_DAYS (safety net).
    """
    if not entry:
        return True
    fetched = datetime.fromisoformat(entry.get("fetched_at", "2000-01-01"))
    if (datetime.now() - fetched).days > CACHE_MAX_AGE_DAYS:
        return True
    next_exp = entry.get("next_expiry")
    return bool(next_exp and date.fromisoformat(next_exp) < date.today())


def _api_fetch_expiries(exchange: str, underlying: str) -> list[str]:
    """
    Call get_expiries for the past 12 months plus the next 2 months.
    Returns a sorted, deduplicated list of ISO date strings.
    """
    today = date.today()
    months: list[tuple[int, int]] = []

    # 12 months back (inclusive of current month)
    y, m = today.year, today.month
    for _ in range(25):    # 24 months back + current → covers Jan 2025 from Jul 2026
        months.append((y, m))
        m -= 1
        if m == 0:
            m, y = 12, y - 1

    # 2 months forward
    y, m = today.year, today.month
    for _ in range(2):
        m += 1
        if m > 12:
            m, y = 1, y + 1
        months.append((y, m))

    results: set[str] = set()
    for y, m in months:
        try:
            resp = call_api(
                get_groww().get_expiries,
                exchange=exchange,
                underlying_symbol=underlying,
                year=y,
                month=m,
            )
            results.update(resp.get("expiries", []))
        except Exception as exc:
            print(f"  get_expiries {underlying} {y}-{m:02d}: {exc}")

    return sorted(results)


def get_expiries_with_cache(exchange: str, underlying: str) -> list[date]:
    """Return all known expiry dates, refreshing from API only when needed."""
    cache = _load_cache()
    entry = cache.get(underlying, {})

    if _cache_needs_refresh(entry):
        print(f"[{underlying}] Refreshing expiry calendar from API...")
        all_exp = _api_fetch_expiries(exchange, underlying)
        today   = date.today()
        future  = [e for e in all_exp if date.fromisoformat(e) >= today]
        cache[underlying] = {
            "expiries":    all_exp,
            "fetched_at":  datetime.now().isoformat(),
            "next_expiry": future[0] if future else None,
        }
        _save_cache(cache)
        print(f"[{underlying}] Cached {len(all_exp)} expiries; next={cache[underlying]['next_expiry']}")
    else:
        print(f"[{underlying}] Using cached expiry calendar.")

    return [date.fromisoformat(e) for e in cache[underlying]["expiries"]]


def get_downloadable_expiries(exchange: str, underlying: str, n: int) -> list[date]:
    """
    Last N expiries that are safe to download:
      - Fully past (< today), OR
      - Today's expiry and current hour >= MARKET_CLOSE_HOUR (9 PM).
    """
    all_expiries = get_expiries_with_cache(exchange, underlying)
    today = date.today()
    now   = datetime.now()
    out: list[date] = []

    for d in sorted(all_expiries, reverse=True):
        if d < today or (d == today and now.hour >= MARKET_CLOSE_HOUR):
            out.append(d)
        if len(out) >= n:
            break

    return out


# ── Spot / ATM helpers ────────────────────────────────────────────────────────
def get_spot_at_open(spot_file: Path, expiry_date: date) -> float | None:
    """Read the first candle at/after 09:15 on expiry_date from the spot CSV."""
    if not spot_file.exists():
        return None
    try:
        df  = pd.read_csv(spot_file, parse_dates=["timestamp"])
        day = df[df["timestamp"].dt.date == expiry_date].sort_values("timestamp")
        row = day[day["timestamp"].dt.time >= pd.Timestamp("09:15").time()]
        return float(row.iloc[0]["close"]) if not row.empty else None
    except Exception:
        return None


def vectorized_atm(spots: np.ndarray, interval: int) -> np.ndarray:
    """
    Compute ATM strikes for an array of spot values in one numpy operation.
    NaN inputs propagate as NaN in the output.
    """
    out = np.full_like(spots, np.nan)
    valid = ~np.isnan(spots)
    out[valid] = np.round(spots[valid] / interval) * interval
    return out


def atm_range(atm: int, interval: int, n: int) -> list[int]:
    """All strikes from ATM - n*interval to ATM + n*interval inclusive."""
    return [atm + (i - n) * interval for i in range(2 * n + 1)]


# ── File I/O ──────────────────────────────────────────────────────────────────
def _save_csv(fp: Path, candles: list) -> int:
    seen: set = set()
    rows = []
    for row in candles:
        if row[0] not in seen:
            seen.add(row[0])
            rows.append(row)
    tmp = Path(str(fp) + ".tmp")
    with open(tmp, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(CSV_HEADERS)
        w.writerows(rows)
    os.replace(tmp, fp)
    return len(rows)


# ── Contract download ─────────────────────────────────────────────────────────
def _has_expiry_day_data(fp: Path, expiry_date: date) -> bool:
    """
    True if the CSV already contains at least one candle on the expiry date.
    A file can exist but have NO expiry-day data when download_data.py fetched
    the full contract history before the expiry occurred — the file is then stale
    and must be re-downloaded for 0DTE purposes.
    """
    try:
        import pandas as pd
        df = pd.read_csv(fp, parse_dates=["timestamp"])
        return any(df["timestamp"].dt.date == expiry_date)
    except Exception:
        return False


def _download_contract(exchange: str, groww_symbol: str,
                       expiry_date: date, out_dir: Path) -> str:
    """
    Download one contract for the expiry day only (09:15–15:30).
    Returns a human-readable status string.

    Skip logic: only skip if the file exists AND already contains expiry-day
    candles. A file with pre-expiry-only data is stale and re-downloaded.
    """
    fp = out_dir / f"{groww_symbol}.csv"
    if fp.exists() and _has_expiry_day_data(fp, expiry_date):
        return f"skip (exists): {groww_symbol}"

    s_str = f"{expiry_date} 09:15:00"
    e_str = f"{expiry_date} 15:30:00"
    try:
        resp = call_api(
            get_groww().get_historical_candles,
            exchange=exchange,
            segment=GrowwAPI.SEGMENT_FNO,
            groww_symbol=groww_symbol,
            start_time=s_str,
            end_time=e_str,
            candle_interval=GrowwAPI.CANDLE_INTERVAL_MIN_1,
        )
        n = _save_csv(fp, resp.get("candles", []))
        return f"saved {n} rows: {groww_symbol}"
    except Exception as exc:
        return f"FAILED {groww_symbol}: {exc}"


def download_expiry(cfg: dict, expiry_date: date) -> None:
    """Download ATM ± ATM_STRIKE_RANGE contracts for one expiry."""
    name     = cfg["name"]
    exchange = cfg["exchange"]
    interval = cfg["interval"]
    out_dir  = BASE_DIR / name / str(expiry_date)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Estimate ATM from spot at market open
    spot = get_spot_at_open(cfg["spot_file"], expiry_date)
    if spot is not None:
        atm_val    = int(vectorized_atm(np.array([spot]), interval)[0])
        target_set = set(atm_range(atm_val, interval, ATM_STRIKE_RANGE))
        print(f"  [{name}] {expiry_date}: spot={spot:.0f}  ATM={atm_val}  ±{ATM_STRIKE_RANGE} strikes")
    else:
        target_set = None
        print(f"  [{name}] {expiry_date}: no spot data — downloading all contracts")

    # Fetch full contract list from API
    try:
        resp = call_api(
            get_groww().get_contracts,
            exchange=exchange,
            underlying_symbol=name,
            expiry_date=str(expiry_date),
        )
        all_contracts: list[str] = resp.get("contracts", [])
    except Exception as exc:
        print(f"  [{name}] {expiry_date}: get_contracts failed: {exc}")
        return

    # Filter to ATM range (skip if no spot — download everything)
    if target_set is not None:
        contracts = [c for c in all_contracts
                     if any(f"-{s}-" in c for s in target_set)]
    else:
        contracts = all_contracts

    already = sum(
        1 for c in contracts
        if (out_dir / f"{c}.csv").exists()
    )
    needed = len(contracts) - already
    print(f"  [{name}] {expiry_date}: {needed} to download, {already} already present "
          f"({len(contracts)}/{len(all_contracts)} in range)")

    if needed == 0:
        return

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_download_contract, exchange, c, expiry_date, out_dir): c
            for c in contracts
        }
        for fut in as_completed(futures):
            result = fut.result()
            if "FAILED" in result:
                print(f"    ✗ {result}")
            elif not result.startswith("skip"):
                print(f"    ✓ {result}")


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    print("=" * 64)
    print(f"0DTE Downloader  —  {N_EXPIRIES} expiries per index")
    print("=" * 64)
    get_groww()  # auth once up front

    for cfg in INDICES:
        name     = cfg["name"]
        exchange = cfg["exchange"]
        print(f"\n[{name}] Resolving expiry calendar...")
        expiries = get_downloadable_expiries(exchange, name, N_EXPIRIES)
        if not expiries:
            print(f"[{name}] No downloadable expiries found.")
            continue
        print(f"[{name}] Will download: {[str(e) for e in expiries]}\n")
        for expiry in expiries:
            download_expiry(cfg, expiry)

    print("\nAll done.")


if __name__ == "__main__":
    main()
