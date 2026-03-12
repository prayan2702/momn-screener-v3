"""
data_service.py
===============
Multi-API data-fetching service for Momentum Screener.
Supports: YFinance (live) | Upstox (LIVE) | Angel One (LIVE) | Zerodha (placeholder)

ANGEL ONE SPEED OPTIMIZATION (v2):
  - ThreadPoolExecutor se parallel requests (5 workers)
  - Token Bucket Rate Limiter (3 req/sec strictly enforce)
  - Fixed sleep hataya — ab network latency parallel mein hide hoti hai
  - Expected speedup: 3-4x faster for large symbol lists
"""

import time
import threading
import requests
import numpy as np
import pandas as pd
import yfinance as yf
import streamlit as st
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from upstox_auth import get_upstox_access_token
from angelone_auth import get_angelone_client

# ─────────────────────────────────────────────────────────────
# SECTION A — UPSTOX INSTRUMENT MASTER
# ─────────────────────────────────────────────────────────────
_INSTRUMENT_MAP = None

def _load_instrument_map() -> dict:
    global _INSTRUMENT_MAP
    if _INSTRUMENT_MAP is not None:
        return _INSTRUMENT_MAP
    if "upstox_instrument_map" in st.session_state:
        _INSTRUMENT_MAP = st.session_state["upstox_instrument_map"]
        return _INSTRUMENT_MAP

    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    try:
        st.sidebar.info("Downloading Upstox instrument master...")
        df   = pd.read_csv(url, compression="gzip", low_memory=False)
        mask = df["instrument_key"].astype(str).str.startswith("NSE_EQ|")
        df   = df[mask].copy()
        mapping = dict(zip(df["tradingsymbol"].astype(str).str.upper(), df["instrument_key"]))
        _INSTRUMENT_MAP = mapping
        st.session_state["upstox_instrument_map"] = mapping
        st.sidebar.success(f"Instrument master loaded - {len(mapping):,} NSE EQ symbols")
        return mapping
    except Exception as e:
        st.sidebar.error(f"Instrument master load failed: {e}")
        return {}

def _get_instrument_key(symbol_ns: str, instrument_map: dict):
    clean = symbol_ns.replace(".NS", "").replace(".BO", "").upper().strip()
    return instrument_map.get(clean)

# ─────────────────────────────────────────────────────────────
# SECTION B — UPSTOX TOKEN VALIDATION
# ─────────────────────────────────────────────────────────────
def _validate_token(access_token: str) -> bool:
    url = "https://api.upstox.com/v3/historical-candle/NSE_EQ%7CINE002A01018/days/1/2025-01-10/2025-01-01"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        return resp.status_code not in (401, 403)
    except Exception:
        return False

# ─────────────────────────────────────────────────────────────
# SECTION C — UPSTOX SINGLE SYMBOL FETCHER (V3)
# ─────────────────────────────────────────────────────────────
def _fetch_upstox_history_live(instrument_key: str, access_token: str, start_date: datetime, end_date: datetime, retries: int = 2):
    encoded_key   = instrument_key.replace("|", "%7C")
    from_date_str = start_date.strftime("%Y-%m-%d")
    to_date_str   = end_date.strftime("%Y-%m-%d")

    url = f"https://api.upstox.com/v3/historical-candle/{encoded_key}/days/1/{to_date_str}/{from_date_str}"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    delay = 1.0
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 429:
                time.sleep(delay * 2); delay *= 2; continue
            if resp.status_code in (401, 403):
                raise ValueError(f"Token invalid (HTTP {resp.status_code})")
            resp.raise_for_status()
            payload = resp.json()
            candles = payload.get("data", {}).get("candles", [])

            if not candles:
                return None

            df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            if df["timestamp"].dt.tz is not None:
                df["timestamp"] = df["timestamp"].dt.tz_localize(None)
            df.set_index("timestamp", inplace=True)
            df.sort_index(inplace=True)
            return df[["open", "high", "low", "close", "volume"]]

        except ValueError:
            raise
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                time.sleep(delay); delay *= 2
        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(delay); delay *= 2
    return None

# ─────────────────────────────────────────────────────────────
# SECTION F — YFINANCE FETCHER 
# ─────────────────────────────────────────────────────────────
def _download_yfinance_chunk(symbols, start_date, max_retries=3, delay=2.0):
    for attempt in range(max_retries):
        try:
            return yf.download(symbols, start=start_date, progress=False, auto_adjust=True, threads=True, multi_level_index=False)
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(delay); delay *= 2
            else:
                raise e

def fetch_yfinance(symbols, start_date, chunk_size, progress_bar, status_text):
    close_chunks, high_chunks, volume_chunks, failed_symbols = [], [], [], []
    total = len(symbols)
    for k in range(0, total, chunk_size):
        progress = min((k + chunk_size) / total, 1.0)
        chunk    = symbols[k:k + chunk_size]
        for attempt in range(3):
            try:
                raw = _download_yfinance_chunk(chunk, start_date)
                close_chunks.append(raw['Close'])
                high_chunks.append(raw['High'])
                volume_chunks.append(raw['Close'] * raw['Volume'])
                break
            except Exception:
                if attempt == 2:
                    failed_symbols.extend(chunk)
        progress_bar.progress(progress)
        status_text.text(f"YFinance: {int(progress*100)}%")
        time.sleep(1.5)

    progress_bar.progress(1.0)
    status_text.text("Download complete!")
    close  = pd.concat(close_chunks,  axis=1) if close_chunks  else pd.DataFrame()
    high   = pd.concat(high_chunks,   axis=1) if high_chunks   else pd.DataFrame()
    volume = pd.concat(volume_chunks, axis=1) if volume_chunks else pd.DataFrame()
    for df in (close, high, volume):
        df.index = pd.to_datetime(df.index)
    return close, high, volume, failed_symbols

# ─────────────────────────────────────────────────────────────
# SECTION H — UPSTOX BULK FETCHER (LIVE)
# ─────────────────────────────────────────────────────────────
UPSTOX_MAX_LOOKBACK_MONTHS = 120

def fetch_upstox(symbols, start_date, end_date, chunk_size, progress_bar, status_text):
    access_token = get_upstox_access_token(sidebar=True)
    if not access_token:
        progress_bar.progress(0.0)
        st.error("Please complete Upstox login in the sidebar first, then retry.")
        st.stop()

    status_text.text("Validating Upstox token...")
    if not _validate_token(access_token):
        st.session_state.pop("upstox_token_data", None)
        st.error("Token expired. Please re-login from sidebar and retry.")
        st.stop()
    st.sidebar.success("Token validated OK")

    upstox_start = end_date - relativedelta(months=UPSTOX_MAX_LOOKBACK_MONTHS)
    if start_date < upstox_start:
        start_date = upstox_start

    instrument_map = _load_instrument_map()
    if not instrument_map:
        st.error("Could not load Upstox instrument master.")
        st.stop()

    close_map, high_map, vol_map = {}, {}, {}
    failed, not_found = [], 0
    total = len(symbols)

    for i, sym in enumerate(symbols):
        progress = (i + 1) / total
        instrument_key = _get_instrument_key(sym, instrument_map)
        if not instrument_key:
            not_found += 1
            failed.append(sym)
        else:
            try:
                df = _fetch_upstox_history_live(instrument_key, access_token, start_date, end_date)
                if df is not None and not df.empty:
                    idx = pd.to_datetime(df.index)
                    close_map[sym] = pd.Series(df['close'].values, index=idx)
                    high_map[sym]  = pd.Series(df['high'].values, index=idx)
                    vol_map[sym]   = pd.Series((df['close']*df['volume']).values, index=idx)
                else:
                    failed.append(sym)
            except ValueError:
                st.session_state.pop("upstox_token_data", None)
                st.error("Token expired mid-download. Re-login from sidebar and retry.")
                st.stop()
            except Exception:
                failed.append(sym)

        if i % 10 == 0 or i == total - 1:
            progress_bar.progress(progress)
            status_text.text(f"Upstox: {int(progress*100)}% | Fetched: {len(close_map)} | Failed: {len(failed)}")
        time.sleep(0.05)

    progress_bar.progress(1.0)
    status_text.text(f"Done - {len(close_map)}/{total} fetched | Not in master: {not_found}")

    all_idx = pd.bdate_range(start=start_date, end=end_date)
    close  = pd.DataFrame({s: v.reindex(all_idx) for s, v in close_map.items()}, index=all_idx)
    high   = pd.DataFrame({s: v.reindex(all_idx) for s, v in high_map.items()},  index=all_idx)
    volume = pd.DataFrame({s: v.reindex(all_idx) for s, v in vol_map.items()},   index=all_idx)
    
    return close, high, volume, failed


# ─────────────────────────────────────────────────────────────
# SECTION H.2 — UPSTOX WEEKLY ATH FETCHER
# ─────────────────────────────────────────────────────────────
ATH_LOOKBACK_YEARS   = 10   # weekly ATH ke liye 10 year history
DAILY_LOOKBACK_MONTHS = 24  # daily data ke liye 24 months sufficient

def _fetch_upstox_weekly_high(instrument_key: str, access_token: str,
                               start_date: datetime, end_date: datetime, retries: int = 2):
    """Upstox V3 weekly candles — ATH ke liye."""
    encoded_key   = instrument_key.replace("|", "%7C")
    from_date_str = start_date.strftime("%Y-%m-%d")
    to_date_str   = end_date.strftime("%Y-%m-%d")
    url = f"https://api.upstox.com/v3/historical-candle/{encoded_key}/weeks/1/{to_date_str}/{from_date_str}"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}

    delay = 1.0
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 429:
                time.sleep(delay * 2); delay *= 2; continue
            if resp.status_code in (401, 403):
                raise ValueError(f"Token invalid (HTTP {resp.status_code})")
            resp.raise_for_status()
            candles = resp.json().get("data", {}).get("candles", [])
            if not candles:
                return None
            df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume", "oi"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            if df["timestamp"].dt.tz is not None:
                df["timestamp"] = df["timestamp"].dt.tz_localize(None)
            df.set_index("timestamp", inplace=True)
            df.sort_index(inplace=True)
            return df["high"]   # sirf high series chahiye ATH ke liye
        except ValueError:
            raise
        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                time.sleep(delay); delay *= 2
        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(delay); delay *= 2
    return None


def fetch_upstox_ath_weekly(symbols, end_date, progress_bar, status_text):
    """Upstox weekly high fetch for ATH — 10 year history."""
    access_token = get_upstox_access_token(sidebar=True)
    if not access_token:
        st.error("Upstox login required for ATH fetch.")
        return pd.DataFrame()

    if not _validate_token(access_token):
        st.session_state.pop("upstox_token_data", None)
        st.error("Token expired. Re-login and retry.")
        return pd.DataFrame()

    ath_start = end_date - relativedelta(years=ATH_LOOKBACK_YEARS)
    instrument_map = _load_instrument_map()
    if not instrument_map:
        return pd.DataFrame()

    high_map = {}
    total    = len(symbols)
    for i, sym in enumerate(symbols):
        instrument_key = _get_instrument_key(sym, instrument_map)
        if instrument_key:
            try:
                s = _fetch_upstox_weekly_high(instrument_key, access_token, ath_start, end_date)
                if s is not None and not s.empty:
                    high_map[sym] = s
            except ValueError:
                pass
            except Exception:
                pass
        if i % 20 == 0 or i == total - 1:
            progress_bar.progress((i + 1) / total)
            status_text.text(f"ATH Weekly (Upstox): {i+1}/{total} | Got: {len(high_map)}")
        time.sleep(0.05)

    progress_bar.progress(1.0)
    status_text.text(f"ATH Weekly done — {len(high_map)}/{total}")
    if not high_map:
        return pd.DataFrame()
    return pd.DataFrame(high_map)


# ═════════════════════════════════════════════════════════════
# SECTION I.5 — ANGEL ONE BULK FETCHER (LIVE) — OPTIMIZED v2
# ═════════════════════════════════════════════════════════════

_ANGELONE_INSTRUMENT_MAP = None

def _load_angelone_instrument_map():
    global _ANGELONE_INSTRUMENT_MAP
    if _ANGELONE_INSTRUMENT_MAP is not None:
        return _ANGELONE_INSTRUMENT_MAP
    
    if "angelone_instrument_map" in st.session_state:
        _ANGELONE_INSTRUMENT_MAP = st.session_state["angelone_instrument_map"]
        return _ANGELONE_INSTRUMENT_MAP

    url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    try:
        st.sidebar.info("Downloading Angel One instrument master...")
        response = requests.get(url, timeout=15)
        data = response.json()
        
        mapping = {}
        for item in data:
            if item['exch_seg'] == 'NSE' and item['symbol'].endswith('-EQ'):
                clean_symbol = item['symbol'].replace('-EQ', '').upper()
                mapping[clean_symbol] = item['token']
                
        _ANGELONE_INSTRUMENT_MAP = mapping
        st.session_state["angelone_instrument_map"] = mapping
        st.sidebar.success(f"Angel One master loaded - {len(mapping):,} NSE EQ symbols")
        return mapping
    except Exception as e:
        st.sidebar.error(f"Angel One master load failed: {e}")
        return {}


# ── Token Bucket Rate Limiter (Thread-Safe) ─────────────────
# Angel One max ~3 requests/sec. Ye class precisely enforce karta hai.
class _TokenBucket:
    """
    Thread-safe token bucket.
    max_rate = 3 req/sec by default (Angel One limit).
    Threads yahan block karte hain jab tak token available nahi hota.
    """
    def __init__(self, max_rate: float = 3.0):
        self._rate      = max_rate          # tokens added per second
        self._tokens    = max_rate          # current available tokens
        self._last_time = time.monotonic()
        self._lock      = threading.Lock()

    def acquire(self):
        """Block until a token is available, then consume one."""
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_time
                # Refill tokens based on elapsed time
                self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
                self._last_time = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
            # Not enough tokens — wait a tiny bit then retry
            time.sleep(0.05)


def _fetch_angelone_history_live(client, token: str, start_date: datetime, end_date: datetime, retries=3):
    """Single-symbol fetch. Thread-safe (client object ki state shared nahi hoti)."""
    historicParam = {
        "exchange": "NSE",
        "symboltoken": token,
        "interval": "ONE_DAY",
        "fromdate": start_date.strftime("%Y-%m-%d 09:15"),
        "todate":   end_date.strftime("%Y-%m-%d 15:30"),
    }

    delay = 1.0
    for attempt in range(retries):
        try:
            resp = client.getCandleData(historicParam)
            if resp.get('status') and resp.get('data'):
                columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                df = pd.DataFrame(resp['data'], columns=columns)
                df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
                df.set_index('timestamp', inplace=True)
                return df[['open', 'high', 'low', 'close', 'volume']]
            return None
        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(delay)
            delay *= 2
    return None


# ── Worker function (runs in each thread) ───────────────────
def _angelone_worker(sym, token, client, start_date, end_date, rate_limiter):
    """
    Rate limit enforce karo, phir data fetch karo.
    Return: (sym, df_or_None)
    """
    rate_limiter.acquire()          # 3/sec limit strictly follow karo
    df = _fetch_angelone_history_live(client, token, start_date, end_date)
    return sym, df


def fetch_angelone(symbols, start_date, end_date, chunk_size, progress_bar, status_text):
    """
    OPTIMIZED Angel One bulk fetcher.
    
    Key changes vs old version:
      OLD: Sequential loop + time.sleep(0.35) per symbol
           → 500 symbols × 0.35s = 175s sirf sleep mein!
      NEW: ThreadPoolExecutor (5 workers) + Token Bucket (3 req/sec)
           → Network latency parallel mein hide hoti hai
           → Expected: 3-4x speedup on large lists
    """
    client = get_angelone_client(sidebar=True)
    if not client:
        progress_bar.progress(0.0)
        st.stop()

    # ── Date cap: Angel One max 2000 days ───────────────────
    angelone_start = end_date - timedelta(days=2000)
    if start_date < angelone_start:
        st.sidebar.info(
            f"Angel One API Limit: Date capped to {angelone_start.strftime('%d-%m-%Y')} "
            f"(Max 2000 days per request allowed)"
        )
        start_date = angelone_start

    status_text.text("Angel One Token Validated. Fetching Master...")
    instrument_map = _load_angelone_instrument_map()
    if not instrument_map:
        st.error("Could not load Angel One instrument master.")
        st.stop()

    # ── Symbol → Token resolution ────────────────────────────
    tasks   = []   # list of (sym, token) to fetch
    failed  = []   # symbols not found in master
    not_found = 0

    for sym in symbols:
        token = instrument_map.get(sym.upper().replace('.NS', ''))
        if not token:
            not_found += 1
            failed.append(sym)
        else:
            tasks.append((sym, token))

    total         = len(symbols)
    fetched_count = 0
    close_map, high_map, vol_map = {}, {}, {}

    # ── Rate limiter shared across all threads ───────────────
    # 2.8 req/sec (slightly under 3) for safety margin
    rate_limiter = _TokenBucket(max_rate=2.5)

    status_text.text(f"Angel One: Starting parallel fetch for {len(tasks)} symbols...")

    # ── Parallel fetch with 5 workers ────────────────────────
    # Why 5? Angel One allows ~3 req/sec. With 5 threads, each thread
    # waits ~1.8s average. Threads spend most time waiting for network
    # response (~0.3-0.8s), so 5 workers keeps the pipeline full.
    MAX_WORKERS = 4

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {
            executor.submit(_angelone_worker, sym, token, client, start_date, end_date, rate_limiter): sym
            for sym, token in tasks
        }

        for future in as_completed(future_map):
            sym_result, df = future.result()
            fetched_count += 1

            if df is not None and not df.empty:
                idx = pd.to_datetime(df.index)
                close_map[sym_result] = pd.Series(df['close'].values,                    index=idx)
                high_map[sym_result]  = pd.Series(df['high'].values,                     index=idx)
                vol_map[sym_result]   = pd.Series((df['close'] * df['volume']).values,   index=idx)
            else:
                failed.append(sym_result)

            # Progress update every 5 completions
            if fetched_count % 5 == 0 or fetched_count == len(tasks):
                progress = (fetched_count + not_found) / total
                progress_bar.progress(min(progress, 1.0))
                status_text.text(
                    f"Angel One: {int(progress * 100)}% | "
                    f"Fetched: {len(close_map)} | Failed: {len(failed)}"
                )

    progress_bar.progress(1.0)
    status_text.text(f"Done — {len(close_map)}/{total} fetched | Not in master: {not_found}")

    all_idx = pd.bdate_range(start=start_date, end=end_date)
    close  = pd.DataFrame({s: v.reindex(all_idx) for s, v in close_map.items()}, index=all_idx)
    high   = pd.DataFrame({s: v.reindex(all_idx) for s, v in high_map.items()},  index=all_idx)
    volume = pd.DataFrame({s: v.reindex(all_idx) for s, v in vol_map.items()},   index=all_idx)

    if close.empty:
        st.error("No data fetched from Angel One. Try re-logging in and retry.")
        st.stop()

    return close, high, volume, failed



# ─────────────────────────────────────────────────────────────
# SECTION I.6 — ANGEL ONE WEEKLY ATH FETCHER
# ─────────────────────────────────────────────────────────────
def _fetch_angelone_weekly_high(client, token: str, start_date: datetime, end_date: datetime, retries=4):
    """Angel One ONE_WEEK candles — ATH ke liye sirf high return karta hai."""
    historicParam = {
        "exchange": "NSE",
        "symboltoken": token,
        "interval": "ONE_WEEK",
        "fromdate": start_date.strftime("%Y-%m-%d 09:15"),
        "todate":   end_date.strftime("%Y-%m-%d 15:30"),
    }
    delay = 2.0
    for attempt in range(retries):
        try:
            resp = client.getCandleData(historicParam)
            if resp.get('status') and resp.get('data'):
                columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
                df = pd.DataFrame(resp['data'], columns=columns)
                df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
                df.set_index('timestamp', inplace=True)
                return df['high']   # sirf high series

            error_code = str(resp.get('errorcode', '') or resp.get('error_code', ''))
            error_msg  = str(resp.get('message', '') or resp.get('msg', '')).lower()
            is_rate_limit = (
                error_code in _RATE_LIMIT_CODES
                or any(kw in error_msg for kw in _RATE_LIMIT_KEYWORDS)
            )
            if is_rate_limit:
                time.sleep(delay * (2 ** attempt))
                continue
            return None
        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(delay * (2 ** attempt))
    return None


def _angelone_weekly_worker(sym, token, client, start_date, end_date, rate_limiter):
    rate_limiter.acquire()
    time.sleep(0.05)
    s = _fetch_angelone_weekly_high(client, token, start_date, end_date)
    return sym, s


def fetch_angelone_ath_weekly(symbols, end_date, progress_bar, status_text):
    """
    Angel One weekly high fetch for ATH — 10 year history.
    ONE_WEEK interval use karta hai, isliye ~520 candles (10yr) — fast fetch.
    Angel One ka 2000-day limit daily ke liye hai; weekly mein longer range allow hota hai.
    """
    client = get_angelone_client(sidebar=True)
    if not client:
        return pd.DataFrame()

    # 10 year ATH start — no artificial cap for weekly
    ath_start = end_date - relativedelta(years=ATH_LOOKBACK_YEARS)

    instrument_map = _load_angelone_instrument_map()
    if not instrument_map:
        return pd.DataFrame()

    tasks = []
    for sym in symbols:
        token = instrument_map.get(sym.upper().replace('.NS', ''))
        if token:
            tasks.append((sym, token))

    high_map     = {}
    total        = len(symbols)
    fetched      = 0
    rate_limiter = _TokenBucket(max_rate=1.5)

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_map = {
            executor.submit(_angelone_weekly_worker, sym, tok, client, ath_start, end_date, rate_limiter): sym
            for sym, tok in tasks
        }
        for future in as_completed(future_map):
            sym_result, s = future.result()
            fetched += 1
            if s is not None and not s.empty:
                high_map[sym_result] = s
            if fetched % 10 == 0 or fetched == len(tasks):
                progress_bar.progress(min(fetched / total, 1.0))
                status_text.text(f"ATH Weekly (Angel One): {fetched}/{len(tasks)} | Got: {len(high_map)}")

    progress_bar.progress(1.0)
    status_text.text(f"ATH Weekly done — {len(high_map)}/{total}")
    if not high_map:
        return pd.DataFrame()
    return pd.DataFrame(high_map)


def fetch_zerodha(symbols, start_date, end_date, chunk_size, progress_bar, status_text):
    status_text.text("Zerodha (MOCK) is not implemented yet.")
    st.stop()

# ─────────────────────────────────────────────────────────────
# SECTION K — UNIFIED ENTRY POINTS
# ─────────────────────────────────────────────────────────────
def fetch_data(api_source, symbols, start_date, end_date,
               chunk_size, progress_bar, status_text) -> tuple:
    if api_source == "YFinance":
        return fetch_yfinance(symbols, start_date, chunk_size, progress_bar, status_text)
    elif api_source == "Upstox":
        return fetch_upstox(symbols, start_date, end_date, chunk_size, progress_bar, status_text)
    elif api_source == "Angel One":
        return fetch_angelone(symbols, start_date, end_date, chunk_size, progress_bar, status_text)
    elif api_source == "Zerodha":
        return fetch_zerodha(symbols, start_date, end_date, chunk_size, progress_bar, status_text)
    else:
        raise ValueError(f"Unknown api_source: {api_source!r}")


def fetch_ath_weekly(api_source, symbols, end_date, progress_bar, status_text) -> pd.DataFrame:
    """
    ATH ke liye weekly high DataFrame fetch karta hai.
    Returns: DataFrame — columns=symbols, index=weekly timestamps.
    ATH = returned_df.max() per column.

    YFinance : 1wk interval (10yr, single call)
    Upstox   : weeks/1 V3 endpoint (10yr)
    Angel One: ONE_WEEK interval (10yr; no daily 2000-day cap here)
    """
    if api_source == "YFinance":
        ath_start = end_date - relativedelta(years=ATH_LOOKBACK_YEARS)
        try:
            raw = yf.download(
                symbols, start=ath_start, end=end_date,
                interval="1wk", progress=False, auto_adjust=True,
                threads=True, multi_level_index=False
            )
            return raw['High'] if 'High' in raw.columns else pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    elif api_source == "Upstox":
        return fetch_upstox_ath_weekly(symbols, end_date, progress_bar, status_text)

    elif api_source == "Angel One":
        return fetch_angelone_ath_weekly(symbols, end_date, progress_bar, status_text)

    else:
        return pd.DataFrame()
