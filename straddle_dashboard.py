#!/usr/bin/env python3
"""
straddle_dashboard.py
=====================
Generates straddle_dashboard.html — a Bootstrap 5 dashboard showing 0DTE
ATM straddle premiums at 9:16 AM and 10:00 AM for the last N_EXPIRIES
of NIFTY and SENSEX, plus spot sparklines and day-type classification.
"""

import csv
import json
import os
import threading
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Optional

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

N_EXPIRIES             = 90
NIFTY_STRIKE_INTERVAL  = 50
SENSEX_STRIKE_INTERVAL = 100
CHECKPOINTS         = [dtime(9, 16), dtime(10, 0)]
MARKET_CLOSE_HOUR      = 21
MAX_WORKERS            = 8
HTML_REFRESH_SECS      = 20
TOKEN_REFRESH_SECS     = 6 * 3600
MAX_RETRIES            = 4
RETRY_BASE_WAIT        = 1.5
EXPIRY_CACHE_FILE      = BASE_DIR / "Data" / "expiry_cache.json"
CACHE_MAX_AGE_DAYS     = 7
OUTPUT_HTML            = BASE_DIR / "straddle_dashboard.html"
DAILY_DATA_START       = date(2024, 1, 1)

CSV_HEADERS = ["timestamp", "open", "high", "low", "close", "volume", "open_interest"]

INDEX_CONFIGS: dict[str, dict] = {
    "NIFTY": {
        "name":        "NIFTY",
        "exchange":    GrowwAPI.EXCHANGE_NSE,
        "symbol":      "NSE-NIFTY",
        "file_prefix": "NSE-NIFTY",
        "spot_file":   BASE_DIR / "Data" / "NIFTY_SPOT.csv",
        "daily_file":  BASE_DIR / "Data" / "NIFTY_DAILY.csv",
        "data_dir":    BASE_DIR / "NIFTY",
        "interval":    NIFTY_STRIKE_INTERVAL,
        "badge_class": "badge-nifty",
        "color":       "#0d6efd",
    },
    "SENSEX": {
        "name":        "SENSEX",
        "exchange":    GrowwAPI.EXCHANGE_BSE,
        "symbol":      "BSE-SENSEX",
        "file_prefix": "BSE-SENSEX",
        "spot_file":   BASE_DIR / "Data" / "SENSEX_SPOT.csv",
        "daily_file":  BASE_DIR / "Data" / "SENSEX_DAILY.csv",
        "data_dir":    BASE_DIR / "SENSEX",
        "interval":    SENSEX_STRIKE_INTERVAL,
        "badge_class": "badge-sensex",
        "color":       "#dc3545",
    },
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  AUTH + RATE LIMITER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  EXPIRY CACHE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
    if not entry:
        return True
    fetched = datetime.fromisoformat(entry.get("fetched_at", "2000-01-01"))
    if (datetime.now() - fetched).days > CACHE_MAX_AGE_DAYS:
        return True
    next_exp = entry.get("next_expiry")
    return bool(next_exp and date.fromisoformat(next_exp) < date.today())


def _api_fetch_expiries(exchange: str, underlying: str) -> list[str]:
    today = date.today()
    months: list[tuple[int, int]] = []
    y, m = today.year, today.month
    for _ in range(25):    # 24 months back + current → covers Jan 2025 from Jul 2026
        months.append((y, m))
        m -= 1
        if m == 0:
            m, y = 12, y - 1
    y, m = today.year, today.month
    for _ in range(2):
        m += 1
        if m > 12:
            m, y = 1, y + 1
        months.append((y, m))

    results: set[str] = set()
    for y, m in months:
        try:
            resp = call_api(get_groww().get_expiries,
                            exchange=exchange,
                            underlying_symbol=underlying,
                            year=y, month=m)
            results.update(resp.get("expiries", []))
        except Exception as exc:
            print(f"  get_expiries {underlying} {y}-{m:02d}: {exc}")
    return sorted(results)


def get_expiries_with_cache(exchange: str, underlying: str) -> list[date]:
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


def get_display_expiries(exchange: str, underlying: str, n: int) -> list[date]:
    all_exp = get_expiries_with_cache(exchange, underlying)
    today = date.today()
    now   = datetime.now()
    out: list[date] = []
    for d in sorted(all_exp, reverse=True):
        if d < today or (d == today and now.hour >= MARKET_CLOSE_HOUR):
            out.append(d)
        if len(out) >= n:
            break
    return out


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SPOT DATA
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def load_spot_df(spot_file: Path) -> Optional[pd.DataFrame]:
    if not spot_file.exists():
        print(f"  WARNING: spot file missing: {spot_file.name}")
        return None
    df = pd.read_csv(spot_file, parse_dates=["timestamp"])
    return df.sort_values("timestamp").reset_index(drop=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DAILY SPOT DATA  (for gap calculation)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _last_date_in_csv(fp: Path) -> Optional[date]:
    if not fp.exists():
        return None
    try:
        with open(fp) as f:
            lines = f.readlines()
        for line in reversed(lines):
            s = line.strip()
            if s and not s.startswith("timestamp"):
                return datetime.fromisoformat(s.split(",")[0]).date()
    except Exception:
        pass
    return None


def download_daily_spot(cfg: dict) -> None:
    """
    Incrementally download daily OHLC candles for an index (SEGMENT_CASH).
    Groww API caps daily candles at 180 days per request, so we chunk.
    The daily close is the official exchange-computed close (weighted average),
    more reliable than the 15:29/15:30 1-min candle for gap calculations.
    """
    name   = cfg["name"]
    output = cfg["daily_file"]
    output.parent.mkdir(parents=True, exist_ok=True)

    last_date  = _last_date_in_csv(output)
    start_date = (last_date + timedelta(days=1)) if last_date else DAILY_DATA_START
    today      = date.today()

    if start_date > today:
        print(f"  [{name} daily] Up to date.")
        return

    # Chunk into 180-day windows
    chunks, cur = [], start_date
    while cur < today:
        end = min(cur + timedelta(days=179), today)
        chunks.append((cur, end))
        cur = end + timedelta(days=1)

    print(f"  [{name} daily] Fetching {start_date} → {today} ({len(chunks)} chunk(s))...")

    all_rows: list = []
    seen:     set  = set()

    for s, e in chunks:
        s_str = f"{s} 00:00:00"
        e_str = f"{e} 23:59:59"
        _rl.acquire()
        try:
            resp = get_groww().get_historical_candles(
                exchange=cfg["exchange"],
                segment=GrowwAPI.SEGMENT_CASH,
                groww_symbol=cfg["symbol"],
                start_time=s_str,
                end_time=e_str,
                candle_interval=GrowwAPI.CANDLE_INTERVAL_DAY,
                timeout=15,
            )
            for row in resp.get("candles", []):
                if row[0] not in seen:
                    seen.add(row[0])
                    all_rows.append(row)
        except Exception as exc:
            print(f"  [{name} daily] chunk {s}→{e} ERROR: {exc}")

    if not all_rows:
        print(f"  [{name} daily] No new data.")
        return

    all_rows.sort(key=lambda r: r[0])
    mode = "a" if (output.exists() and last_date) else "w"
    with open(output, mode, newline="") as f:
        w = csv.writer(f)
        if mode == "w":
            w.writerow(CSV_HEADERS)
        w.writerows(all_rows)
    print(f"  [{name} daily] +{len(all_rows)} rows → {output.name}")


def load_daily_df(daily_file: Path) -> Optional[pd.DataFrame]:
    if not daily_file.exists():
        return None
    df = pd.read_csv(daily_file, parse_dates=["timestamp"])
    return df.sort_values("timestamp").reset_index(drop=True)


def compute_gap(daily_df: Optional[pd.DataFrame],
                spot_df: Optional[pd.DataFrame],
                expiry_date: date) -> tuple[Optional[float], Optional[float]]:
    """
    Return (gap_pts, gap_pct):
      gap_pts = today's market open  − previous trading day's official close
      gap_pct = gap_pts / prev_close × 100

    prev_close: from daily candle (official weighted-average close, more
                accurate than any single 1-min candle at 15:29/15:30).
    today_open: from the first 1-min spot candle at 9:15 AM (the true
                opening price — Groww daily candles set open = prev_close
                so they carry no gap information).
    """
    if daily_df is None or daily_df.empty:
        return None, None

    # Previous trading day's close from daily data
    prev_rows = daily_df[daily_df["timestamp"].dt.date < expiry_date]
    if prev_rows.empty:
        return None, None
    prev_close = float(prev_rows.iloc[-1]["close"])

    # Today's open from first 1-min candle
    if spot_df is None:
        return None, None
    day = spot_df[spot_df["timestamp"].dt.date == expiry_date].sort_values("timestamp")
    first_candle = day[day["timestamp"].dt.time >= pd.Timestamp("09:15").time()]
    if first_candle.empty:
        return None, None
    today_open = float(first_candle.iloc[0]["open"])

    gap_pts = round(today_open - prev_close, 2)
    gap_pct = round(gap_pts / prev_close * 100, 3) if prev_close else None
    return gap_pts, gap_pct


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ATM  (vectorized)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def vectorized_atm(spots: np.ndarray, interval: int) -> np.ndarray:
    out   = np.full_like(spots, np.nan)
    valid = ~np.isnan(spots)
    out[valid] = np.round(spots[valid] / interval) * interval
    return out


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CONTRACT FILE HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _expiry_str(d: date) -> str:
    return d.strftime("%d%b%y")


def contract_path(cfg: dict, expiry: date, strike: int, opt: str) -> Path:
    fname = f"{cfg['file_prefix']}-{_expiry_str(expiry)}-{strike}-{opt}.csv"
    return cfg["data_dir"] / str(expiry) / fname


def contract_symbol(cfg: dict, expiry: date, strike: int, opt: str) -> str:
    return f"{cfg['file_prefix']}-{_expiry_str(expiry)}-{strike}-{opt}"


def read_close_at(fp: Path, target_date: date, target_time: dtime) -> Optional[float]:
    try:
        df  = pd.read_csv(fp, parse_dates=["timestamp"])
        day = df[df["timestamp"].dt.date == target_date]
        row = day[day["timestamp"] >= datetime.combine(target_date, target_time)]
        return float(row.iloc[0]["close"]) if not row.empty else None
    except Exception:
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DOWNLOAD HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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


def _download_contract(cfg: dict, expiry: date, strike: int, opt: str) -> tuple[bool, str]:
    fp  = contract_path(cfg, expiry, strike, opt)
    sym = contract_symbol(cfg, expiry, strike, opt)
    if fp.exists():
        return True, "already exists"
    fp.parent.mkdir(parents=True, exist_ok=True)
    s_str = f"{expiry} 09:15:00"
    e_str = f"{expiry} 15:30:00"
    try:
        resp = call_api(
            get_groww().get_historical_candles,
            exchange=cfg["exchange"],
            segment=GrowwAPI.SEGMENT_FNO,
            groww_symbol=sym,
            start_time=s_str,
            end_time=e_str,
            candle_interval=GrowwAPI.CANDLE_INTERVAL_MIN_1,
        )
        n = _save_csv(fp, resp.get("candles", []))
        return True, f"downloaded {n} rows"
    except Exception as exc:
        return False, str(exc)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  CELL COMPUTATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_MOVE_KEYS = ("open_to_high", "open_to_low", "high_to_low")
_MOVE_NONE = {k: None for k in _MOVE_KEYS}


def _blank_cell(atm=None):
    return {"premium": None, "atm": atm, "error": None, "pending": False, **_MOVE_NONE}

def _error_cell(msg, atm=None):
    return {"premium": None, "atm": atm, "error": msg,  "pending": False, **_MOVE_NONE}

def _pending_cell(atm=None):
    return {"premium": None, "atm": atm, "error": None, "pending": True,  **_MOVE_NONE}


def compute_movements(spot_df: Optional[pd.DataFrame], expiry: date,
                      t: dtime, ref_price: float) -> dict:
    """Index movement metrics from time t onwards on expiry day."""
    if spot_df is None:
        return _MOVE_NONE.copy()
    day   = spot_df[spot_df["timestamp"].dt.date == expiry]
    after = day[day["timestamp"] >= datetime.combine(expiry, t)]
    if after.empty:
        return _MOVE_NONE.copy()
    day_high = float(after["high"].max())
    day_low  = float(after["low"].min())
    return {
        "open_to_high": round(day_high - ref_price, 2),
        "open_to_low":  round(ref_price - day_low,  2),
        "high_to_low":  round(day_high - day_low,   2),
    }


def _is_empty_csv(fp: Path) -> bool:
    """True if the file exists but contains no data rows (header-only marker)."""
    try:
        return pd.read_csv(fp).empty
    except Exception:
        return True


def compute_checkpoint_cell(cfg: dict, expiry: date, atm: Optional[int],
                             t: dtime, spot_df: Optional[pd.DataFrame] = None,
                             ref_price: Optional[float] = None) -> dict:
    if atm is None:
        return _error_cell("No spot data")

    interval = cfg["interval"]

    # If the exact ATM files are missing, trigger download (pending)
    ce_fp0 = contract_path(cfg, expiry, atm, "CE")
    pe_fp0 = contract_path(cfg, expiry, atm, "PE")
    if not ce_fp0.exists() or not pe_fp0.exists():
        return _pending_cell(atm)

    # Try ATM first, then adjacent strikes if ATM has zero candles.
    # Empty CSVs mean that specific strike had no trades on expiry day —
    # common for less-liquid SENSEX expiries. Walk outward up to ±3 strikes.
    for delta in [0, -1, 1, -2, 2, -3, 3]:
        strike = atm + delta * interval
        ce_fp  = contract_path(cfg, expiry, strike, "CE")
        pe_fp  = contract_path(cfg, expiry, strike, "PE")

        # Only consider strikes already downloaded on both sides
        if not ce_fp.exists() or not pe_fp.exists():
            continue
        if _is_empty_csv(ce_fp) or _is_empty_csv(pe_fp):
            continue

        ce_price = read_close_at(ce_fp, expiry, t)
        pe_price = read_close_at(pe_fp, expiry, t)
        if ce_price is None or pe_price is None:
            continue

        premium = round(ce_price + pe_price, 2)
        cell = {"premium": premium, "atm": strike, "error": None,
                "pending": False, **_MOVE_NONE}
        if ref_price is not None:
            cell.update(compute_movements(spot_df, expiry, t, ref_price))
        return cell

    # No liquid strike found within ±3 of ATM
    return _error_cell(f"No liquid data near ATM {atm}", atm)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SPARKLINE + DAY CLASSIFICATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def load_spark_data(spot_df: Optional[pd.DataFrame],
                    expiry: date) -> tuple[list[float], list[str]]:
    """Return (close_prices, time_labels) for the full expiry day session."""
    if spot_df is None:
        return [], []
    day = spot_df[spot_df["timestamp"].dt.date == expiry].sort_values("timestamp")
    closes = [round(float(v), 2) for v in day["close"].tolist()]
    times  = [ts.strftime("%H:%M") for ts in day["timestamp"]]
    return closes, times


def classify_day(c916: dict) -> tuple[str, str]:
    """Trending / Volatile / Consolidating based on 9:16 AM metrics."""
    o2h = c916.get("open_to_high")
    o2l = c916.get("open_to_low")
    h2l = c916.get("high_to_low")
    straddle = c916.get("premium")

    if any(v is None for v in [o2h, o2l, h2l, straddle]) or straddle == 0:
        return "—", "Insufficient data"

    range_mult     = h2l / straddle
    larger         = max(o2h, o2l)
    smaller        = min(o2h, o2l)
    direction_bias = larger / smaller if smaller > 0 else 99.0
    direction      = "up" if o2h >= o2l else "down"

    if range_mult < 0.8:
        return (
            "Consolidating",
            f"Range of {h2l:.0f} pts ({range_mult:.2f}x straddle) — "
            f"index stayed within straddle bounds all day"
        )
    if direction_bias >= 2.5 and range_mult >= 1.0:
        return (
            "Trending",
            f"Strong {direction}trend — ↑ {o2h:.0f} vs ↓ {o2l:.0f} pts; "
            f"total range {range_mult:.2f}x straddle with {direction_bias:.1f}:1 directional bias"
        )
    return (
        "Volatile",
        f"Range of {h2l:.0f} pts ({range_mult:.2f}x straddle) "
        f"with roughly equal up/down swings — ↑ {o2h:.0f} vs ↓ {o2l:.0f} pts"
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DATA COLLECTION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def collect_all_rows(spot_dfs: dict[str, Optional[pd.DataFrame]],
                     expiries_map: dict[str, list[date]],
                     daily_dfs: dict[str, Optional[pd.DataFrame]]) -> list[dict]:
    rows: list[dict] = []

    # --- vectorized ATM (one numpy pass per index) ---
    spots_map: dict[str, np.ndarray] = {}
    atm_map:   dict[str, np.ndarray] = {}

    for idx_name, cfg in INDEX_CONFIGS.items():
        expiries = expiries_map[idx_name]
        spot_df  = spot_dfs[idx_name]
        spots    = np.full((len(expiries), len(CHECKPOINTS)), np.nan)

        if spot_df is not None:
            for i, exp in enumerate(expiries):
                day = spot_df[spot_df["timestamp"].dt.date == exp]
                for j, t in enumerate(CHECKPOINTS):
                    row = day[day["timestamp"] >= datetime.combine(exp, t)]
                    if not row.empty:
                        spots[i, j] = float(row.iloc[0]["close"])

        spots_map[idx_name] = spots
        atm_map[idx_name]   = vectorized_atm(spots, cfg["interval"])

    # --- parallel cell computation ---
    task_args = [
        (idx_name, i, j)
        for idx_name in INDEX_CONFIGS
        for i in range(len(expiries_map[idx_name]))
        for j in range(len(CHECKPOINTS))
    ]

    cell_results: dict[tuple, dict] = {}

    def _compute_cell(args):
        idx_name, i, j = args
        cfg       = INDEX_CONFIGS[idx_name]
        expiry    = expiries_map[idx_name][i]
        atm_f     = atm_map[idx_name][i, j]
        atm       = int(atm_f) if not np.isnan(atm_f) else None
        ref_f     = spots_map[idx_name][i, j]
        ref_price = float(ref_f) if not np.isnan(ref_f) else None
        return args, compute_checkpoint_cell(
            cfg, expiry, atm, CHECKPOINTS[j],
            spot_df=spot_dfs[idx_name], ref_price=ref_price,
        )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for args, cell in pool.map(_compute_cell, task_args):
            cell_results[args] = cell

    # --- assemble rows ---
    chk_keys = [f"c{t.hour:02d}{t.minute:02d}" for t in CHECKPOINTS]

    for idx_name, cfg in INDEX_CONFIGS.items():
        spot_df = spot_dfs[idx_name]
        for i, expiry in enumerate(expiries_map[idx_name]):
            row = {
                "index":       idx_name,
                "expiry":      expiry,
                "rank":        i + 1,
                "badge_class": cfg["badge_class"],
                "color":       cfg["color"],
            }
            for j, key in enumerate(chk_keys):
                row[key] = cell_results[(idx_name, i, j)]

            closes, times = load_spark_data(spot_df, expiry)
            row["sparkline"]   = closes
            row["spark_times"] = times
            row["day_type"], row["day_reason"] = classify_day(row[chk_keys[0]])

            # Gap: prev-day daily close → expiry-day 9:15 open (1-min candle)
            gap_pts, gap_pct = compute_gap(daily_dfs.get(idx_name), spot_df, expiry)
            row["gap_pts"] = gap_pts
            row["gap_pct"] = gap_pct

            rows.append(row)

    rows.sort(key=lambda r: r["expiry"], reverse=True)
    return rows


def find_pending_downloads(rows: list[dict]) -> list[tuple[dict, date, int, str]]:
    chk_keys = [f"c{t.hour:02d}{t.minute:02d}" for t in CHECKPOINTS]
    downloads: list[tuple] = []
    seen: set[tuple] = set()

    for row in rows:
        cfg    = INDEX_CONFIGS[row["index"]]
        expiry = row["expiry"]
        for key in chk_keys:
            cell = row[key]
            if cell["pending"] and cell["atm"] is not None:
                atm = cell["atm"]
                for opt in ("CE", "PE"):
                    fp    = contract_path(cfg, expiry, atm, opt)
                    token = (expiry, atm, opt, row["index"])
                    if not fp.exists() and token not in seen:
                        seen.add(token)
                        downloads.append((cfg, expiry, atm, opt))
    return downloads


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HTML GENERATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _fmt_time(t: dtime) -> str:
    return t.strftime("%I:%M %p").lstrip("0")

_CHECKPOINT_LABELS = {f"c{t.hour:02d}{t.minute:02d}": _fmt_time(t) for t in CHECKPOINTS}

DAY_BADGE = {
    "Trending":      "bg-warning text-dark",
    "Volatile":      "bg-danger",
    "Consolidating": "bg-success",
    "—":             "bg-secondary",
}


def _render_movements(cell: dict) -> str:
    premium = cell["premium"]
    o2h = cell.get("open_to_high")
    o2l = cell.get("open_to_low")
    h2l = cell.get("high_to_low")
    if o2h is None and o2l is None and h2l is None:
        return ""

    def fmt(pts: Optional[float], prefix: str) -> str:
        if pts is None:
            return ""
        x = f"{pts / premium:.2f}x" if premium else "—"
        return (f'<span class="mvt-item">{prefix} <strong>{pts:.0f}</strong> '
                f'<span class="text-muted">({x})</span></span>')

    inner = " &thinsp;·&thinsp; ".join(
        p for p in [fmt(o2h, "↑"), fmt(o2l, "↓"), fmt(h2l, "↔")] if p
    )
    return f'<div class="mvt-row">{inner}</div>'


def _render_cell(cell: dict) -> str:
    if cell["pending"]:
        atm_txt = f" (ATM {cell['atm']})" if cell["atm"] else ""
        return f'<span class="cell-pending">⏳ Fetching{atm_txt}…</span>'
    if cell["error"]:
        return f'<span class="cell-error">❌ {cell["error"]}</span>'
    if cell["premium"] is None:
        return '<span class="cell-na">—</span>'
    atm_txt = (f' <small class="text-muted">(ATM {cell["atm"]})</small>'
               if cell["atm"] else "")
    return f'<strong>{cell["premium"]:.2f}</strong>{atm_txt}' + _render_movements(cell)


def _row_nums(row: dict) -> str:
    """JSON of numeric values used by JS for average row computation."""
    def ratio(v, p):
        return round(v / p, 4) if (v is not None and p) else None

    c0, c1 = row["c0916"], row["c1000"]
    p0, p1  = c0.get("premium"), c1.get("premium")

    return json.dumps({
        "prem916":  p0,
        "o2h916":   c0.get("open_to_high"),
        "o2h916x":  ratio(c0.get("open_to_high"), p0),
        "o2l916":   c0.get("open_to_low"),
        "o2l916x":  ratio(c0.get("open_to_low"),  p0),
        "h2l916":   c0.get("high_to_low"),
        "h2l916x":  ratio(c0.get("high_to_low"),  p0),
        "prem1000": p1,
        "o2h1000":  c1.get("open_to_high"),
        "o2h1000x": ratio(c1.get("open_to_high"), p1),
        "o2l1000":  c1.get("open_to_low"),
        "o2l1000x": ratio(c1.get("open_to_low"),  p1),
        "h2l1000":  c1.get("high_to_low"),
        "h2l1000x": ratio(c1.get("high_to_low"),  p1),
        "gap_pts":  row.get("gap_pts"),
        "gap_pct":  row.get("gap_pct"),
    })


def _render_gap(gap_pts: Optional[float], gap_pct: Optional[float]) -> str:
    if gap_pts is None:
        return '<span class="cell-na">—</span>'
    color = "text-success" if gap_pts >= 0 else "text-danger"
    sign  = "+" if gap_pts >= 0 else ""
    pct_s = f" ({sign}{gap_pct:.2f}%)" if gap_pct is not None else ""
    return (f'<span class="{color} fw-semibold">{sign}{gap_pts:.2f}</span>'
            f'<small class="text-muted">{pct_s}</small>')


def _render_rows(rows: list[dict]) -> str:
    chk_keys = [f"c{t.hour:02d}{t.minute:02d}" for t in CHECKPOINTS]
    lines = []
    for row in rows:
        badge      = f'<span class="badge {row["badge_class"]}">{row["index"]}</span>'
        expiry_fmt = f"{row['expiry'].day} {row['expiry'].strftime('%b %Y')}"
        cells      = "".join(f"<td>{_render_cell(row[k])}</td>" for k in chk_keys)
        dt         = row.get("day_type", "—")
        reason     = row.get("day_reason", "")
        dt_cls     = DAY_BADGE.get(dt, "bg-secondary")
        spark_j    = json.dumps(row.get("sparkline",   []))
        times_j    = json.dumps(row.get("spark_times", []))
        nums_j     = _row_nums(row)
        color      = row.get("color", "#0d6efd")
        gap_td     = (f'<td class="text-nowrap">'
                      f'{_render_gap(row.get("gap_pts"), row.get("gap_pct"))}</td>')

        lines.append(
            f'    <tr data-index="{row["index"]}" data-rank="{row["rank"]}"'
            f' data-color="{color}"'
            f' data-spark=\'{spark_j}\' data-times=\'{times_j}\''
            f' data-nums=\'{nums_j}\'>\n'
            f'      <td>{badge}</td>\n'
            f'      <td class="text-nowrap">{expiry_fmt}</td>\n'
            f'      {cells}\n'
            f'      {gap_td}\n'
            f'      <td class="spark-cell align-middle text-center px-2"></td>\n'
            f'      <td class="analysis-col align-middle text-center">'
            f'<span class="badge {dt_cls}">{dt}</span></td>\n'
            f'      <td class="analysis-col" style="min-width:260px;font-size:0.8em">{reason}</td>\n'
            f'    </tr>'
        )
    return "\n".join(lines)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HTML TEMPLATE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  {refresh_tag}
  <title>0DTE Straddle Dashboard</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
        rel="stylesheet" crossorigin="anonymous">
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"
          crossorigin="anonymous"></script>
  <style>
    body          {{ background-color:#f8f9fa; }}
    .badge-nifty  {{ background-color:#0d6efd; }}
    .badge-sensex {{ background-color:#dc3545; }}
    .cell-pending {{ color:#e6a817; font-style:italic; }}
    .cell-error   {{ color:#dc3545; font-size:0.85em; }}
    .cell-na      {{ color:#adb5bd; }}
    .mvt-row      {{ margin-top:4px; font-size:0.82em; color:#495057; }}
    .mvt-item     {{ white-space:nowrap; }}
    th            {{ white-space:nowrap; }}
    .spark-cell   {{ min-width:100px; }}
    .spark-svg    {{ cursor:pointer; display:block; margin:auto; }}
    .spark-svg:hover {{ opacity:0.8; }}
    tfoot td      {{ background:#f0f0f0; font-weight:600; border-top:2px solid #dee2e6; }}
    tfoot .mvt-row{{ font-weight:normal; }}
    /* Analysis columns always extend beyond the viewport — scroll right to reveal */
    #main-table   {{ min-width: calc(100vw + 520px); }}
    .analysis-col {{ min-width: 260px; }}
    /* Vol score mini-grid */
                     padding:3px 5px; border-radius:4px; min-width:38px;
                     font-size:0.78em; line-height:1.3; }}
                     margin-top:3px; }}
  </style>
</head>
<body>
<div class="container-fluid py-4 px-4">

  <!-- Header -->
  <div class="d-flex justify-content-between align-items-center mb-3">
    <h5 class="mb-0 fw-semibold">&#128202; 0DTE Straddle Dashboard</h5>
    <small class="text-muted">Updated: {updated_at}{download_notice}</small>
  </div>

  <!-- Controls -->
  <div class="card mb-3 shadow-sm border-0">
    <div class="card-body py-2 px-3">
      <div class="row align-items-center g-3">

        <div class="col-auto d-flex align-items-center gap-2">
          <span class="fw-semibold text-secondary small">INDEX</span>
          <div class="btn-group btn-group-sm" role="group">
            <input type="radio" class="btn-check" name="idx" id="idx-all" value="all" checked>
            <label class="btn btn-outline-secondary" for="idx-all">Combined</label>
            <input type="radio" class="btn-check" name="idx" id="idx-nifty" value="NIFTY">
            <label class="btn btn-outline-primary" for="idx-nifty">Nifty</label>
            <input type="radio" class="btn-check" name="idx" id="idx-sensex" value="SENSEX">
            <label class="btn btn-outline-danger" for="idx-sensex">Sensex</label>
          </div>
        </div>

        <div class="col-auto d-flex align-items-center gap-2">
          <span class="fw-semibold text-secondary small">EXPIRIES</span>
          <select id="exp-count" class="form-select form-select-sm" style="width:auto">
            <option value="5">5</option>
            <option value="10" selected>10</option>
            <option value="15">15</option>
            <option value="0">Custom</option>
          </select>
          <input type="number" id="exp-custom"
                 class="form-control form-control-sm d-none"
                 min="1" max="100" placeholder="N" style="width:72px">
        </div>

        <div class="col-auto ms-auto">
          <small class="text-muted">&#8594; scroll table to see Day Type &amp; Reason</small>
        </div>

      </div>
    </div>
  </div>

  <!-- Table -->
  <div class="card shadow-sm border-0">
    <div class="table-responsive">
      <table class="table table-hover table-striped align-middle mb-0" id="main-table">
        <thead class="table-dark">
          <tr>
            <th>Index</th>
            <th>Expiry</th>
            {checkpoint_headers}
            <th>Gap</th>
            <th class="text-center">Spot Chart</th>
            <th class="analysis-col text-center">Day Type</th>
            <th class="analysis-col">Reason &nbsp;<span class="text-muted fw-normal" style="font-size:0.75em">&#8592; scroll</span></th>
          </tr>
        </thead>
        <tbody id="tbl">
{rows}
        </tbody>
        <tfoot>
          <tr id="avg-row">
            <td colspan="2" class="text-secondary" style="font-size:0.85em">
              &#8709; Average <span id="avg-count" class="text-muted"></span>
            </td>
            {avg_cells}
            <td id="avg-gap">—</td>
            <td></td>
            <td class="analysis-col"></td>
            <td class="analysis-col"></td>
          </tr>
        </tfoot>
      </table>
    </div>
  </div>

</div>

<!-- Chart Modal -->
<div class="modal fade" id="chartModal" tabindex="-1">
  <div class="modal-dialog modal-lg modal-dialog-centered">
    <div class="modal-content">
      <div class="modal-header py-2">
        <h6 class="modal-title fw-semibold" id="chartModalLabel"></h6>
        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
      </div>
      <div class="modal-body p-3">
        <canvas id="chartCanvas" height="120"></canvas>
      </div>
    </div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"
        crossorigin="anonymous"></script>
<script>
// ── Sparklines ────────────────────────────────────────────────────────────────
function renderSparklines() {{
  document.querySelectorAll('#tbl tr[data-spark]').forEach(row => {{
    const closes = JSON.parse(row.dataset.spark || '[]');
    const cell   = row.querySelector('.spark-cell');
    if (!cell) return;
    if (closes.length < 2) {{ cell.innerHTML = '<span class="cell-na">—</span>'; return; }}

    const W = 90, H = 30;
    const mn = Math.min(...closes), mx = Math.max(...closes), rng = mx - mn || 1;
    const pts = closes.map((v, i) =>
      `${{(i / (closes.length-1) * W).toFixed(1)}},${{(H - (v-mn)/rng * H).toFixed(1)}}`
    ).join(' ');
    const color = row.dataset.color || '#0d6efd';
    cell.innerHTML =
      `<svg width="${{W}}" height="${{H}}" class="spark-svg" title="Click for detail">` +
      `<polyline points="${{pts}}" fill="none" stroke="${{color}}" stroke-width="1.5"` +
      ` stroke-linejoin="round"/></svg>`;
    cell.querySelector('svg').addEventListener('click', () => openChart(row));
  }});
}}

// ── Detail chart modal ────────────────────────────────────────────────────────
let _chart = null;
function openChart(row) {{
  const closes = JSON.parse(row.dataset.spark || '[]');
  const times  = JSON.parse(row.dataset.times || '[]');
  const expiry = row.querySelector('td:nth-child(2)').textContent.trim();
  const color  = row.dataset.color || '#0d6efd';
  document.getElementById('chartModalLabel').textContent =
    row.dataset.index + '  ' + expiry + '  (9:15 – 15:30)';

  const ctx = document.getElementById('chartCanvas').getContext('2d');
  if (_chart) _chart.destroy();
  _chart = new Chart(ctx, {{
    type: 'line',
    data: {{
      labels: times,
      datasets: [{{ data: closes, borderColor: color, borderWidth: 1.5,
                    pointRadius: 0, tension: 0.1,
                    label: row.dataset.index }}]
    }},
    options: {{
      responsive: true,
      plugins: {{ legend: {{ display: false }},
                  tooltip: {{ mode: 'index', intersect: false }} }},
      scales: {{
        x: {{ ticks: {{ maxTicksLimit: 13, font: {{ size: 10 }} }} }},
        y: {{ ticks: {{ font: {{ size: 10 }} }} }}
      }}
    }}
  }});
  new bootstrap.Modal(document.getElementById('chartModal')).show();
}}

// ── Average row ───────────────────────────────────────────────────────────────
function fmtAvg(v, dec) {{ return v !== null ? v.toFixed(dec) : '—'; }}

function mvtHtml(p, h, hx, l, lx, hl, hlx) {{
  if (p === null) return '—';
  return `<strong>${{fmtAvg(p,2)}}</strong>` +
    `<div class="mvt-row">` +
    `<span class="mvt-item">&#8593; <strong>${{fmtAvg(h,0)}}</strong> ` +
    `<span class="text-muted">(${{fmtAvg(hx,2)}}x)</span></span>` +
    ` &thinsp;&middot;&thinsp; ` +
    `<span class="mvt-item">&#8595; <strong>${{fmtAvg(l,0)}}</strong> ` +
    `<span class="text-muted">(${{fmtAvg(lx,2)}}x)</span></span>` +
    ` &thinsp;&middot;&thinsp; ` +
    `<span class="mvt-item">&#8596; <strong>${{fmtAvg(hl,0)}}</strong> ` +
    `<span class="text-muted">(${{fmtAvg(hlx,2)}}x)</span></span>` +
    `</div>`;
}}

function updateAverage() {{
  const visible = Array.from(
    document.querySelectorAll('#tbl tr[data-index]')
  ).filter(r => r.style.display !== 'none');

  const flds = ['prem916','o2h916','o2h916x','o2l916','o2l916x','h2l916','h2l916x',
                'prem1000','o2h1000','o2h1000x','o2l1000','o2l1000x','h2l1000','h2l1000x',
                'gap_pts','gap_pct'];
  const sums = {{}}, cnts = {{}};
  flds.forEach(f => {{ sums[f] = 0; cnts[f] = 0; }});

  visible.forEach(row => {{
    const d = JSON.parse(row.dataset.nums || '{{}}');
    flds.forEach(f => {{
      const v = d[f];
      if (v !== null && v !== undefined && !isNaN(v)) {{ sums[f] += v; cnts[f]++; }}
    }});
  }});

  const avg = f => cnts[f] > 0 ? sums[f] / cnts[f] : null;

  const a916  = document.getElementById('avg-c0916');
  const a1000 = document.getElementById('avg-c1000');
  if (a916)  a916.innerHTML  = mvtHtml(avg('prem916'), avg('o2h916'), avg('o2h916x'),
                                        avg('o2l916'), avg('o2l916x'),
                                        avg('h2l916'), avg('h2l916x'));
  if (a1000) a1000.innerHTML = mvtHtml(avg('prem1000'), avg('o2h1000'), avg('o2h1000x'),
                                        avg('o2l1000'), avg('o2l1000x'),
                                        avg('h2l1000'), avg('h2l1000x'));

  const cnt = document.getElementById('avg-count');
  if (cnt) cnt.textContent = '(' + visible.length + ' rows)';

  // Gap average
  const agap = document.getElementById('avg-gap');
  const gp = avg('gap_pts'), gc = avg('gap_pct');
  if (agap) {{
    if (gp !== null) {{
      const color = gp >= 0 ? 'text-success' : 'text-danger';
      const sign  = gp >= 0 ? '+' : '';
      agap.innerHTML = `<span class="${{color}} fw-semibold">${{sign}}${{fmtAvg(gp,2)}}</span>` +
                       `<small class="text-muted"> (${{sign}}${{fmtAvg(gc,2)}}%)</small>`;
    }} else {{ agap.textContent = '—'; }}
  }}
}}

// ── Filter + average (called on every control change) ─────────────────────────
function applyFilters() {{
  const idx = document.querySelector('input[name="idx"]:checked').value;
  const sel = document.getElementById('exp-count').value;
  const n   = sel === '0'
    ? (parseInt(document.getElementById('exp-custom').value) || 0)
    : parseInt(sel);

  const rows = document.querySelectorAll('#tbl tr[data-index]');
  const cnt  = {{}};
  rows.forEach(row => {{
    const ri = row.dataset.index;
    if (idx !== 'all' && ri !== idx) {{ row.style.display = 'none'; return; }}
    cnt[ri] = (cnt[ri] || 0) + 1;
    row.style.display = (n === 0 || cnt[ri] <= n) ? '' : 'none';
  }});
  updateAverage();
}}

// ── Init ──────────────────────────────────────────────────────────────────────
document.querySelectorAll('input[name="idx"]').forEach(r =>
  r.addEventListener('change', applyFilters));
document.getElementById('exp-count').addEventListener('change', function() {{
  document.getElementById('exp-custom').classList.toggle('d-none', this.value !== '0');
  applyFilters();
}});
document.getElementById('exp-custom').addEventListener('input', applyFilters);

renderSparklines();
applyFilters();
</script>
</body>
</html>
"""


def generate_html(rows: list[dict], pending_count: int) -> str:
    chk_keys = [f"c{t.hour:02d}{t.minute:02d}" for t in CHECKPOINTS]

    checkpoint_headers = "".join(
        f'<th>Straddle {_CHECKPOINT_LABELS[k]}</th>' for k in chk_keys
    )
    avg_cells = "".join(
        f'<td id="avg-{k}">—</td>' for k in chk_keys
    )


    if pending_count > 0:
        refresh_tag     = f'<meta http-equiv="refresh" content="{HTML_REFRESH_SECS}">'
        download_notice = (f' &nbsp;·&nbsp; <span class="text-warning">'
                           f'⏳ Downloading {pending_count} contract(s)…</span>')
    else:
        refresh_tag     = ""
        download_notice = ""

    now = datetime.now()
    updated_at = (f"{now.day} {now.strftime('%b %Y')}, "
                  f"{now.strftime('%I:%M %p').lstrip('0')}")

    return _HTML_TEMPLATE.format(
        refresh_tag        = refresh_tag,
        updated_at         = updated_at,
        download_notice    = download_notice,
        checkpoint_headers = checkpoint_headers,
        avg_cells          = avg_cells,
        rows               = _render_rows(rows),
    )


def write_html(rows: list[dict], pending_count: int) -> None:
    html = generate_html(rows, pending_count)
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    status = f"({pending_count} download(s) pending)" if pending_count else "(final)"
    print(f"HTML written → {OUTPUT_HTML.name}  {status}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MAIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def main() -> None:
    print("=" * 64)
    print("0DTE Straddle Dashboard")
    print("=" * 64)

    get_groww()

    # 1. Resolve expiry lists
    expiries_map: dict[str, list[date]] = {}
    for idx_name, cfg in INDEX_CONFIGS.items():
        expiries_map[idx_name] = get_display_expiries(
            cfg["exchange"], idx_name, N_EXPIRIES
        )
        print(f"[{idx_name}] {len(expiries_map[idx_name])} expiries: "
              f"{[str(e) for e in expiries_map[idx_name]]}")

    # 2. Download + load daily spot data (for gap calculation)
    print("\nUpdating daily spot data...")
    for cfg in INDEX_CONFIGS.values():
        download_daily_spot(cfg)

    daily_dfs: dict[str, Optional[pd.DataFrame]] = {
        idx_name: load_daily_df(cfg["daily_file"])
        for idx_name, cfg in INDEX_CONFIGS.items()
    }

    # 3. Load intraday spot DataFrames
    spot_dfs: dict[str, Optional[pd.DataFrame]] = {
        idx_name: load_spot_df(cfg["spot_file"])
        for idx_name, cfg in INDEX_CONFIGS.items()
    }

    # 4. Compute rows
    print("\nComputing straddle premiums and movements...")
    rows = collect_all_rows(spot_dfs, expiries_map, daily_dfs)

    # 5. Find missing contracts
    pending = find_pending_downloads(rows)

    # 5. Write interim HTML immediately
    write_html(rows, pending_count=len(pending))

    if not pending:
        print(f"\nDone. Open: {OUTPUT_HTML}")
        return

    print(f"\n{len(pending)} contract file(s) missing — downloading in parallel...")

    # 6. Dispatch downloads
    pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    future_map: dict[Future, tuple] = {
        pool.submit(_download_contract, cfg, expiry, strike, opt):
            (cfg, expiry, strike, opt)
        for cfg, expiry, strike, opt in pending
    }

    # 7. Wait and track failures
    failed: set[tuple] = set()
    for fut in as_completed(future_map):
        cfg, expiry, strike, opt = future_map[fut]
        try:
            ok, msg = fut.result()
            sym = contract_symbol(cfg, expiry, strike, opt)
            print(f"  {'✓' if ok else '✗'} {sym}: {msg}")
            if not ok:
                failed.add((cfg["name"], expiry, strike, opt))
        except Exception as exc:
            failed.add((cfg["name"], expiry, strike, opt))
            print(f"  ✗ exception: {exc}")

    pool.shutdown(wait=True)

    # 8. Recompute pending cells
    chk_keys = [f"c{t.hour:02d}{t.minute:02d}" for t in CHECKPOINTS]
    for row in rows:
        cfg    = INDEX_CONFIGS[row["index"]]
        expiry = row["expiry"]
        for j, key in enumerate(chk_keys):
            if not row[key]["pending"]:
                continue
            atm = row[key]["atm"]
            if atm is None:
                continue
            ce_failed = (row["index"], expiry, atm, "CE") in failed
            pe_failed = (row["index"], expiry, atm, "PE") in failed
            if ce_failed or pe_failed:
                details = ", ".join(
                    o for o in ("CE", "PE")
                    if (row["index"], expiry, atm, o) in failed
                )
                row[key] = _error_cell(f"Download failed ({details})", atm)
            else:
                row[key] = compute_checkpoint_cell(
                    cfg, expiry, atm, CHECKPOINTS[j],
                    spot_df=spot_dfs[row["index"]],
                    ref_price=None,
                )
        # Re-classify after recompute
        row["day_type"], row["day_reason"] = classify_day(row[chk_keys[0]])

    # 9. Write final HTML
    write_html(rows, pending_count=0)
    print(f"\nDone. Open: {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
