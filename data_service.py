"""
data_service.py
===============
Multi-API data-fetching service for Momentum Screener.
Supports: YFinance (live) | Upstox (LIVE) | Zerodha (placeholder)

KEY FIX v7:
  Upstox "Invalid date range" error was because start_date = 2000-01-01
  was too far back. Upstox has a per-request date range limit.

  Solution: For Upstox, automatically cap start_date to 2 years ago.
  Momentum screener only needs 12M data anyway — 2 years is more than enough.

  V3 correct URL format for daily candles:
    /v3/historical-candle/{key}/days/1/{to_date}/{from_date}
"""

import time
import requests
import numpy as np
import pandas as pd
import yfinance as yf
import streamlit as st
import requests
from angelone_auth import get_angelone_client
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from upstox_auth import get_upstox_access_token


# ─────────────────────────────────────────────────────────────
# SECTION A — INSTRUMENT MASTER
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
# SECTION B — TOKEN VALIDATION
# ─────────────────────────────────────────────────────────────
def _validate_token(access_token: str) -> bool:
    """Validate token with 1 real API call using correct V3 URL + safe date range."""
    url = (
        "https://api.upstox.com/v3/historical-candle"
        "/NSE_EQ%7CINE002A01018/days/1/2025-01-10/2025-01-01"
    )
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        return resp.status_code not in (401, 403)
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────
# SECTION C — SINGLE SYMBOL FETCHER (V3)
# ─────────────────────────────────────────────────────────────
def _fetch_upstox_history_live(
    instrument_key: str,
    access_token: str,
    start_date: datetime,
    end_date: datetime,
    retries: int = 2,
):
    """
    Fetch daily OHLCV for one symbol via Upstox V3 API.

    Correct V3 URL:
      /v3/historical-candle/{key}/days/1/{to_date}/{from_date}

    NOTE: start_date must not be older than ~2 years from today.
          Upstox returns "Invalid date range" for very old start dates.
          The caller (fetch_upstox) caps this automatically.
    """
    encoded_key   = instrument_key.replace("|", "%7C")
    from_date_str = start_date.strftime("%Y-%m-%d")
    to_date_str   = end_date.strftime("%Y-%m-%d")

    url = (
        f"https://api.upstox.com/v3/historical-candle"
        f"/{encoded_key}/days/1/{to_date_str}/{from_date_str}"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept":        "application/json",
    }

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

            df = pd.DataFrame(candles, columns=[
                "timestamp", "open", "high", "low", "close", "volume", "oi"
            ])
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
# SECTION D — DEBUG: Show first symbol response
# ─────────────────────────────────────────────────────────────
def _debug_first_symbol(instrument_key, access_token, start_date, end_date):
    encoded_key   = instrument_key.replace("|", "%7C")
    url = (
        f"https://api.upstox.com/v3/historical-candle"
        f"/{encoded_key}/days/1"
        f"/{end_date.strftime('%Y-%m-%d')}/{start_date.strftime('%Y-%m-%d')}"
    )
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        with st.sidebar.expander("Debug: First symbol API response", expanded=True):
            st.write(f"**URL:** `{url}`")
            st.write(f"**HTTP Status:** {resp.status_code}")
            try:
                payload = resp.json()
                candles = payload.get("data", {}).get("candles", [])
                st.write(f"**Candles returned:** {len(candles)}")
                if candles:
                    st.write("**First candle:**", candles[0])
                    st.write("**Last candle:**", candles[-1])
                    st.success(f"Data OK! {len(candles)} candles received.")
                else:
                    st.write("**Full response:**", payload)
            except Exception:
                st.write("**Raw text:**", resp.text[:500])
    except Exception as e:
        st.sidebar.error(f"Debug call failed: {e}")


# ─────────────────────────────────────────────────────────────
# SECTION E — ZERODHA PLACEHOLDER
# ─────────────────────────────────────────────────────────────
def _init_zerodha_client():
    return None

def _fetch_zerodha_history(client, symbol, start_date, end_date):
    if client is None:
        return _generate_mock_ohlcv(symbol, start_date, end_date)

def _generate_mock_ohlcv(symbol, start_date, end_date):
    rng  = pd.date_range(start=start_date, end=end_date, freq='B')
    n    = len(rng)
    seed = abs(hash(symbol)) % (2**31)
    rs   = np.random.RandomState(seed)
    close  = 100 * np.cumprod(1 + rs.normal(0.0003, 0.015, n))
    return pd.DataFrame({
        'open':   close * (1 + rs.normal(0, 0.008, n)),
        'high':   close * (1 + rs.uniform(0, 0.03, n)),
        'low':    close * (1 - rs.uniform(0, 0.03, n)),
        'close':  close,
        'volume': rs.randint(50_000, 5_000_000, n).astype(float),
    }, index=rng)


# ─────────────────────────────────────────────────────────────
# SECTION F — YFINANCE FETCHER (unchanged)
# ─────────────────────────────────────────────────────────────
def _download_yfinance_chunk(symbols, start_date, max_retries=3, delay=2.0):
    for attempt in range(max_retries):
        try:
            return yf.download(
                symbols, start=start_date, progress=False,
                auto_adjust=True, threads=True, multi_level_index=False
            )
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
# SECTION G — WIDE FRAME BUILDER
# ─────────────────────────────────────────────────────────────
def _build_wide_frames(symbol_dfs: dict) -> tuple:
    close_map, high_map, vol_map = {}, {}, {}
    for sym, df in symbol_dfs.items():
        if df is None or df.empty:
            continue
        df.index = pd.to_datetime(df.index)
        close_map[sym] = df['close']
        high_map[sym]  = df['high']
        vol_map[sym]   = df['close'] * df['volume']
    return pd.DataFrame(close_map), pd.DataFrame(high_map), pd.DataFrame(vol_map)


# ─────────────────────────────────────────────────────────────
# SECTION H — UPSTOX BULK FETCHER (LIVE)
# ─────────────────────────────────────────────────────────────

# Upstox daily data limit = 10 years per request
# We use full 10 years but build DataFrames incrementally (no symbol_dfs dict)
# so memory usage stays low — same pattern as YFinance chunk approach
UPSTOX_MAX_LOOKBACK_MONTHS = 120  # 120 months(10 year) — enough for 200DMA + 12M ROC, safe RAM


def fetch_upstox(symbols, start_date, end_date, chunk_size, progress_bar, status_text):

    # STEP 1: Get token
    access_token = get_upstox_access_token(sidebar=True)
    if not access_token:
        progress_bar.progress(0.0)
        st.error("Please complete Upstox login in the sidebar first, then retry.")
        st.stop()

    # STEP 2: Validate token
    status_text.text("Validating Upstox token...")
    if not _validate_token(access_token):
        st.session_state.pop("upstox_token_data", None)
        st.error("Token expired. Please re-login from sidebar and retry.")
        st.stop()
    st.sidebar.success("Token validated OK")

    # STEP 3: CAP start_date for Upstox
    # Upstox gives "Invalid date range" if start_date is too old.
    # Momentum screener only needs 12M data — cap to 14 months back.
    upstox_start = end_date - relativedelta(months=UPSTOX_MAX_LOOKBACK_MONTHS)
    if start_date < upstox_start:
        st.sidebar.info(
            f"Upstox date range capped: {upstox_start.strftime('%d-%m-%Y')} "
            f"to {end_date.strftime('%d-%m-%Y')} "
            f"(screener needs only 12M data)"
        )
        start_date = upstox_start

    # STEP 4: Instrument master
    instrument_map = _load_instrument_map()
    if not instrument_map:
        st.error("Could not load Upstox instrument master.")
        st.stop()

    # STEP 5: Debug first symbol
    first_sym = symbols[0] if symbols else None
    if first_sym:
        first_key = _get_instrument_key(first_sym, instrument_map)
        if first_key:
            _debug_first_symbol(first_key, access_token, start_date, end_date)

    # STEP 6: Bulk fetch — INCREMENTAL (memory efficient, same as YFinance pattern)
    # Do NOT store full DataFrames in a dict — append only 3 Series per symbol
    # This prevents holding 2104 DataFrames in RAM simultaneously
    close_map = {}
    high_map  = {}
    vol_map   = {}
    failed    = []
    not_found = 0
    total     = len(symbols)

    for i, sym in enumerate(symbols):
        progress = (i + 1) / total

        instrument_key = _get_instrument_key(sym, instrument_map)
        if not instrument_key:
            not_found += 1
            failed.append(sym)
        else:
            try:
                df = _fetch_upstox_history_live(
                    instrument_key, access_token, start_date, end_date
                )
                if df is not None and not df.empty:
                    idx = pd.to_datetime(df.index)
                    close_map[sym] = pd.Series(df['close'].values,              index=idx)
                    high_map[sym]  = pd.Series(df['high'].values,               index=idx)
                    vol_map[sym]   = pd.Series((df['close']*df['volume']).values, index=idx)
                    del df   # free immediately — don't hold full OHLCV in memory
                else:
                    failed.append(sym)

            except ValueError:
                st.session_state.pop("upstox_token_data", None)
                st.error("Token expired mid-download. Re-login from sidebar and retry.")
                st.stop()

            except Exception:
                failed.append(sym)

        # Update UI every 10 symbols
        if i % 10 == 0 or i == total - 1:
            progress_bar.progress(progress)
            status_text.text(
                f"Upstox: {int(progress*100)}%  |  "
                f"Fetched: {len(close_map)}  |  "
                f"Failed: {len(failed)}  |  "
                f"Not in master: {not_found}"
            )

        time.sleep(0.05)

    progress_bar.progress(1.0)
    status_text.text(f"Done - {len(close_map)}/{total} fetched | Not in master: {not_found}")

    # Build wide DataFrames — pre-align to common business-day index
    # pd.DataFrame(dict_of_series) is slow when indices differ across 2104 symbols
    # Pre-building a common index and reindexing each Series is 10x faster
    all_idx = pd.bdate_range(start=start_date, end=end_date)
    close  = pd.DataFrame({s: v.reindex(all_idx) for s, v in close_map.items()}, index=all_idx)
    high   = pd.DataFrame({s: v.reindex(all_idx) for s, v in high_map.items()},  index=all_idx)
    volume = pd.DataFrame({s: v.reindex(all_idx) for s, v in vol_map.items()},   index=all_idx)
    del close_map, high_map, vol_map

    if close.empty:
        st.error(
            "No data fetched from Upstox.\n\n"
            "Check sidebar Debug section.\n"
            "Try re-logging in and retry."
        )
        st.stop()

    return close, high, volume, failed


# ─────────────────────────────────────────────────────────────
# SECTION I — ZERODHA BULK FETCHER (Phase-1 mock)
# ─────────────────────────────────────────────────────────────
def fetch_zerodha(symbols, start_date, end_date, chunk_size, progress_bar, status_text):
    client, symbol_dfs = _init_zerodha_client(), {}
    total = len(symbols)
    for i, sym in enumerate(symbols):
        try:
            symbol_dfs[sym] = _fetch_zerodha_history(client, sym.replace('.NS',''), start_date, end_date)
        except Exception:
            symbol_dfs[sym] = None
        if i % 10 == 0:
            progress_bar.progress((i+1)/total)
            status_text.text(f"Zerodha: {int((i+1)/total*100)}%")
        time.sleep(0.05)
    progress_bar.progress(1.0)
    status_text.text("Zerodha (MOCK) complete!")
    close, high, volume = _build_wide_frames(symbol_dfs)
    failed = [s for s, df in symbol_dfs.items() if df is None or df.empty]
    return close, high, volume, failed
# ─────────────────────────────────────────────────────────────
# SECTION I.5 — ANGEL ONE BULK FETCHER (LIVE)
# ─────────────────────────────────────────────────────────────
import pyotp
from SmartApi import SmartConnect

_ANGELONE_INSTRUMENT_MAP = None

def _init_angelone_client():
    """Initialize SmartConnect and login using credentials from secrets."""
    try:
        api_key = st.secrets["angelone"]["api_key"]
        client_code = st.secrets["angelone"]["client_code"]
        password = st.secrets["angelone"]["password"]
        totp_secret = st.secrets["angelone"]["totp_secret"]
    except KeyError:
        st.error("Angel One credentials missing in `.streamlit/secrets.toml`.")
        return None

    try:
        obj = SmartConnect(api_key=api_key)
        totp = pyotp.TOTP(totp_secret).now()
        data = obj.generateSession(client_code, password, totp)
        
        if data['status']:
            return obj
        else:
            st.error(f"Angel One Login Failed: {data.get('message')}")
            return None
    except Exception as e:
        st.error(f"Angel One initialization error: {e}")
        return None

def _load_angelone_instrument_map():
    """Fetch and cache Angel One instrument master."""
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
        
        # Filter for NSE EQ and create mapping: 'RELIANCE-EQ' -> '2885'
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

def _fetch_angelone_history_live(client, token: str, start_date: datetime, end_date: datetime, retries=3):
    """Fetch historical daily candles for a single symbol."""
    historicParam = {
        "exchange": "NSE",
        "symboltoken": token,
        "interval": "ONE_DAY",
        "fromdate": start_date.strftime("%Y-%m-%d %H:%M"),
        "todate": end_date.strftime("%Y-%m-%d %H:%M")
    }
    
    delay = 1.0
    for attempt in range(retries):
        try:
            resp = client.getCandleData(historicParam)
            
            # Rate limit checking (adjust based on Angel One limits, usually 3 requests/sec)
            if resp.get('errorcode') == 'AB1014': # Example rate limit code
                time.sleep(delay * 2)
                delay *= 2
                continue
                
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

def fetch_angelone(symbols, start_date, end_date, chunk_size, progress_bar, status_text):
    """Bulk fetcher for Angel One, matching the Upstox memory-efficient pattern."""
    client = _init_angelone_client()
    if not client:
        st.stop()
        
    instrument_map = _load_angelone_instrument_map()
    if not instrument_map:
        st.error("Could not load Angel One instrument master.")
        st.stop()

    close_map, high_map, vol_map = {}, {}, {}
    failed, not_found = [], 0
    total = len(symbols)

    for i, sym in enumerate(symbols):
        progress = (i + 1) / total
        token = instrument_map.get(sym.upper().replace('.NS', ''))
        
        if not token:
            not_found += 1
            failed.append(sym)
        else:
            df = _fetch_angelone_history_live(client, token, start_date, end_date)
            if df is not None and not df.empty:
                idx = pd.to_datetime(df.index)
                close_map[sym] = pd.Series(df['close'].values, index=idx)
                high_map[sym]  = pd.Series(df['high'].values, index=idx)
                vol_map[sym]   = pd.Series((df['close']*df['volume']).values, index=idx)
            else:
                failed.append(sym)

        # Rate limiting control (Angel One allows ~3 req/sec for historical data)
        time.sleep(0.35)

        if i % 5 == 0 or i == total - 1:
            progress_bar.progress(progress)
            status_text.text(f"Angel One: {int(progress*100)}% | Fetched: {len(close_map)} | Failed: {len(failed)}")

    progress_bar.progress(1.0)
    status_text.text(f"Done - {len(close_map)}/{total} fetched | Not in master: {not_found}")

    all_idx = pd.bdate_range(start=start_date, end=end_date)
    close  = pd.DataFrame({s: v.reindex(all_idx) for s, v in close_map.items()}, index=all_idx)
    high   = pd.DataFrame({s: v.reindex(all_idx) for s, v in high_map.items()},  index=all_idx)
    volume = pd.DataFrame({s: v.reindex(all_idx) for s, v in vol_map.items()},   index=all_idx)

    return close, high, volume, failed

# ─────────────────────────────────────────────────────────────
# SECTION J — UNIFIED ENTRY POINT
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
