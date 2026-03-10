"""
data_service.py
===============
Multi-API data-fetching service for Momentum Screener.
Supports: YFinance (live) | Upstox (LIVE ✅) | Zerodha (placeholder)

Output contract (all APIs must return this shape):
  close   : pd.DataFrame  — index=DatetimeIndex, columns=ticker symbols
  high    : pd.DataFrame  — same shape
  volume  : pd.DataFrame  — same shape  (price × volume, i.e. value in ₹)

Upstox is now LIVE.  Zerodha remains Phase-1 mock.
"""

import time
import requests
import numpy as np
import pandas as pd
import yfinance as yf
import streamlit as st
from datetime import datetime, date
from pathlib import Path

# ── Upstox auth helper (new file you added to repo) ──────────
from upstox_auth import get_upstox_access_token


# ─────────────────────────────────────────────────────────────
# SECTION A ─ UPSTOX INSTRUMENT KEY CACHE
# Upstox v2 uses "instrument_key" like "NSE_EQ|INE002A01018"
# We build a symbol → instrument_key map from Upstox's master CSV
# ─────────────────────────────────────────────────────────────

# Local cache so we only download the instrument master once per session
_INSTRUMENT_MAP: dict | None = None   # { "RELIANCE": "NSE_EQ|INE002A01018", ... }

def _load_instrument_map() -> dict:
    """
    Download Upstox NSE instrument master and build
    { trading_symbol: instrument_key } mapping.

    The master file is ~5 MB but we cache it in session_state.
    """
    global _INSTRUMENT_MAP

    # 1. Already loaded this session
    if _INSTRUMENT_MAP is not None:
        return _INSTRUMENT_MAP

    # 2. Check Streamlit session cache
    if "upstox_instrument_map" in st.session_state:
        _INSTRUMENT_MAP = st.session_state["upstox_instrument_map"]
        return _INSTRUMENT_MAP

    # 3. Download fresh
    url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    try:
        df = pd.read_csv(url, compression="gzip")
        # Columns: instrument_key, exchange, tradingsymbol, name, ...
        # Keep only EQ series to avoid duplicates (futures, options, etc.)
        df = df[df["instrument_type"] == "EQ"].copy()
        mapping = dict(zip(df["tradingsymbol"].str.upper(), df["instrument_key"]))
        _INSTRUMENT_MAP = mapping
        st.session_state["upstox_instrument_map"] = mapping
        return mapping
    except Exception as e:
        st.warning(f"⚠️ Could not load Upstox instrument master: {e}")
        return {}


def _get_instrument_key(symbol_ns: str, instrument_map: dict) -> str | None:
    """
    Convert Yahoo-style ticker (e.g. 'RELIANCE.NS') to Upstox instrument_key.
    Returns None if not found.
    """
    # Strip .NS suffix
    clean = symbol_ns.replace(".NS", "").replace(".BO", "").upper().strip()
    return instrument_map.get(clean)


# ─────────────────────────────────────────────────────────────
# SECTION B ─ UPSTOX HISTORICAL DATA FETCHER (LIVE)
# ─────────────────────────────────────────────────────────────
UPSTOX_HIST_URL = (
    "https://api.upstox.com/v3/historical-candle"
    "/{instrument_key}/{interval}/{to_date}/{from_date}"
)

def _fetch_upstox_history_live(
    instrument_key: str,
    access_token: str,
    start_date: datetime,
    end_date: datetime,
    interval: str = "day",
    retries: int = 3,
) -> pd.DataFrame | None:
    """
    Fetch OHLCV for ONE symbol from Upstox v2 Historical Candle API.

    Parameters
    ----------
    instrument_key : Upstox key, e.g. "NSE_EQ|INE002A01018"
    access_token   : valid Bearer token
    interval       : "day" | "week" | "month" | "1minute" | "30minute" etc.

    Returns
    -------
    DataFrame with DatetimeIndex and columns [open, high, low, close, volume]
    or None on failure.
    """
    from_date_str = start_date.strftime("%Y-%m-%d")
    to_date_str   = end_date.strftime("%Y-%m-%d")

    # URL-encode the pipe character in instrument_key
    encoded_key = instrument_key.replace("|", "%7C")
    url = (
        f"https://api.upstox.com/v3/historical-candle"
        f"/{encoded_key}/{interval}/{to_date_str}/{from_date_str}"
    )

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept":        "application/json",
    }

    delay = 1.0
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=15)

            # Rate limit: back off and retry
            if resp.status_code == 429:
                time.sleep(delay * 2)
                delay *= 2
                continue

            resp.raise_for_status()
            data = resp.json()

            candles = data.get("data", {}).get("candles", [])
            if not candles:
                return None

            # Upstox candle format: [timestamp, open, high, low, close, volume, oi]
            df = pd.DataFrame(candles, columns=[
                "timestamp", "open", "high", "low", "close", "volume", "oi"
            ])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df.set_index("timestamp", inplace=True)
            df.index = df.index.tz_localize(None)   # remove timezone for compatibility
            df.sort_index(inplace=True)
            return df[["open", "high", "low", "close", "volume"]]

        except requests.exceptions.Timeout:
            if attempt < retries - 1:
                time.sleep(delay)
                delay *= 2
        except Exception as e:
            if attempt == retries - 1:
                raise e
            time.sleep(delay)
            delay *= 2

    return None


# ─────────────────────────────────────────────────────────────
# SECTION C ─ ZERODHA SDK PLACEHOLDER (unchanged — Phase-1)
# ─────────────────────────────────────────────────────────────
ZERODHA_CONFIG = {
    "api_key":      "YOUR_KITE_API_KEY",
    "api_secret":   "YOUR_KITE_API_SECRET",
    "access_token": None,
}

def _init_zerodha_client():
    return None  # Phase-1

def _fetch_zerodha_history(client, symbol: str, start_date: datetime, end_date: datetime):
    if client is None:
        return _generate_mock_ohlcv(symbol, start_date, end_date)


# ─────────────────────────────────────────────────────────────
# SECTION D ─ MOCK DATA GENERATOR (fallback / Zerodha Phase-1)
# ─────────────────────────────────────────────────────────────
def _generate_mock_ohlcv(symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    rng   = pd.date_range(start=start_date, end=end_date, freq='B')
    n     = len(rng)
    seed  = abs(hash(symbol)) % (2**31)
    rs    = np.random.RandomState(seed)

    close  = 100 * np.cumprod(1 + rs.normal(0.0003, 0.015, n))
    high   = close * (1 + rs.uniform(0, 0.03, n))
    low    = close * (1 - rs.uniform(0, 0.03, n))
    open_  = close * (1 + rs.normal(0, 0.008, n))
    volume = rs.randint(50_000, 5_000_000, n).astype(float)

    return pd.DataFrame({
        'open': open_, 'high': high, 'low': low,
        'close': close, 'volume': volume,
    }, index=rng)


# ─────────────────────────────────────────────────────────────
# SECTION E ─ YFINANCE FETCHER (unchanged from original)
# ─────────────────────────────────────────────────────────────
def _download_yfinance_chunk(symbols: list, start_date: datetime,
                              max_retries: int = 3, delay: float = 2.0) -> pd.DataFrame:
    for attempt in range(max_retries):
        try:
            return yf.download(
                symbols, start=start_date,
                progress=False, auto_adjust=True,
                threads=True, multi_level_index=False
            )
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2
            else:
                raise e


def fetch_yfinance(symbols: list, start_date: datetime, chunk_size: int,
                   progress_bar, status_text) -> tuple:
    close_chunks, high_chunks, volume_chunks = [], [], []
    failed_symbols = []
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
        status_text.text(f"YFinance downloading… {int(progress*100)}%")
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
# SECTION F ─ WIDE FRAME BUILDER (common for Upstox / Zerodha)
# ─────────────────────────────────────────────────────────────
def _build_wide_frames(symbol_dfs: dict) -> tuple:
    """
    symbol_dfs : {ticker: DataFrame(DatetimeIndex, cols=[open,high,low,close,volume])}
    Returns    : (close_df, high_df, volume_df) — wide format matching YFinance output
    """
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
# SECTION G ─ UPSTOX BULK FETCHER (LIVE)
# ─────────────────────────────────────────────────────────────
def fetch_upstox(symbols: list, start_date: datetime, end_date: datetime,
                 chunk_size: int, progress_bar, status_text) -> tuple:
    """
    Live Upstox data fetch for all symbols.

    - Authenticates via upstox_auth.get_upstox_access_token()
    - Loads instrument master to map tickers → instrument_keys
    - Fetches historical daily candles per symbol
    - Returns wide-format DataFrames matching YFinance contract
    """
    # ── 1. Get access token ──────────────────────────────────────
    access_token = get_upstox_access_token(sidebar=True)
    if not access_token:
        status_text.text("⚠️ Upstox not authenticated. Check sidebar.")
        progress_bar.progress(0.0)
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), symbols

    # ── 2. Load instrument master ────────────────────────────────
    status_text.text("Loading Upstox instrument master…")
    instrument_map = _load_instrument_map()

    symbol_dfs = {}
    failed     = []
    total      = len(symbols)

    # ── 3. Fetch per symbol ──────────────────────────────────────
    for i, sym in enumerate(symbols):
        progress = (i + 1) / total

        instrument_key = _get_instrument_key(sym, instrument_map)
        if not instrument_key:
            failed.append(sym)
            symbol_dfs[sym] = None
            if i % max(1, total // 20) == 0:
                progress_bar.progress(progress)
                status_text.text(f"Upstox: {int(progress*100)}% | ⚠️ No key for {sym}")
            continue

        try:
            df = _fetch_upstox_history_live(
                instrument_key, access_token, start_date, end_date
            )
            symbol_dfs[sym] = df
            if df is None or df.empty:
                failed.append(sym)
        except Exception as e:
            st.warning(f"Upstox: failed for {sym} — {e}")
            symbol_dfs[sym] = None
            failed.append(sym)

        if i % max(1, total // 20) == 0:
            progress_bar.progress(progress)
            status_text.text(f"Upstox downloading… {int(progress*100)}%")

        # Light throttle: Upstox rate limit is ~10 req/sec
        time.sleep(0.12)

    progress_bar.progress(1.0)
    fetched = total - len(failed)
    status_text.text(f"✅ Upstox done — {fetched}/{total} symbols fetched.")

    close, high, volume = _build_wide_frames(symbol_dfs)
    return close, high, volume, failed


# ─────────────────────────────────────────────────────────────
# SECTION H ─ ZERODHA BULK FETCHER (Phase-1 mock — unchanged)
# ─────────────────────────────────────────────────────────────
def fetch_zerodha(symbols: list, start_date: datetime, end_date: datetime,
                  chunk_size: int, progress_bar, status_text) -> tuple:
    client     = _init_zerodha_client()
    symbol_dfs = {}
    total      = len(symbols)

    for i, sym in enumerate(symbols):
        progress  = (i + 1) / total
        clean_sym = sym.replace('.NS', '')
        try:
            df = _fetch_zerodha_history(client, clean_sym, start_date, end_date)
            symbol_dfs[sym] = df
        except Exception as e:
            st.warning(f"Zerodha: failed for {clean_sym} — {e}")
            symbol_dfs[sym] = None

        if i % max(1, total // 20) == 0:
            progress_bar.progress(progress)
            status_text.text(f"Zerodha downloading… {int(progress*100)}%")
        time.sleep(0.05)

    progress_bar.progress(1.0)
    status_text.text("Zerodha (MOCK) download complete!")

    close, high, volume = _build_wide_frames(symbol_dfs)
    failed = [s for s, df in symbol_dfs.items() if df is None or df.empty]
    return close, high, volume, failed


# ─────────────────────────────────────────────────────────────
# SECTION I ─ UNIFIED ENTRY POINT  (called from main app)
# ─────────────────────────────────────────────────────────────
def fetch_data(
    api_source: str,
    symbols: list,
    start_date: datetime,
    end_date: datetime,
    chunk_size: int,
    progress_bar,
    status_text,
) -> tuple:
    """
    Single hook the main app calls regardless of chosen API.

    Parameters
    ----------
    api_source   : "YFinance" | "Upstox" | "Zerodha"
    symbols      : list of Yahoo-format tickers (e.g. ["RELIANCE.NS", ...])
    start_date   : datetime
    end_date     : datetime
    chunk_size   : int  (used for YFinance; Upstox/Zerodha fetch per-symbol)
    progress_bar : st.progress() widget
    status_text  : st.empty()    widget

    Returns
    -------
    (close, high, volume, failed_symbols)
    close/high/volume : pd.DataFrame — wide format, DatetimeIndex, columns=tickers
    failed_symbols    : list[str]
    """
    if api_source == "YFinance":
        return fetch_yfinance(symbols, start_date, chunk_size, progress_bar, status_text)

    elif api_source == "Upstox":
        return fetch_upstox(symbols, start_date, end_date, chunk_size, progress_bar, status_text)

    elif api_source == "Zerodha":
        return fetch_zerodha(symbols, start_date, end_date, chunk_size, progress_bar, status_text)

    else:
        raise ValueError(f"Unknown api_source: {api_source!r}")
