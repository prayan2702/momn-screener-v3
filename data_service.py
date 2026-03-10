"""
data_service.py
===============
Multi-API data-fetching service for Momentum Screener.
Supports: YFinance (live) | Upstox (LIVE ✅) | Zerodha (placeholder)

Output contract (all APIs must return this shape):
  close   : pd.DataFrame  — index=DatetimeIndex, columns=ticker symbols
  high    : pd.DataFrame  — same shape
  volume  : pd.DataFrame  — same shape  (price x volume, i.e. value in Rs)

Upstox is now LIVE.  Zerodha remains Phase-1 mock.

Fixes v3:
  - Timezone fix: tz_convert(None) instead of tz_localize(None)
  - Instrument master: filter by instrument_key prefix (NSE_EQ|)
  - st.stop() if not authenticated or no data — prevents IndexError crash
"""

import time
import requests
import numpy as np
import pandas as pd
import yfinance as yf
import streamlit as st
from datetime import datetime
from pathlib import Path

from upstox_auth import get_upstox_access_token


# ─────────────────────────────────────────────────────────────
# SECTION A - UPSTOX INSTRUMENT KEY CACHE
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
        df = pd.read_csv(url, compression="gzip", low_memory=False)

        # Filter: keep only NSE Equity stocks
        # instrument_key for NSE equity always starts with "NSE_EQ|"
        mask = df["instrument_key"].astype(str).str.startswith("NSE_EQ|")
        df   = df[mask].copy()

        mapping = dict(
            zip(df["tradingsymbol"].astype(str).str.upper(), df["instrument_key"])
        )

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
# SECTION B - UPSTOX HISTORICAL DATA FETCHER (LIVE, V3 API)
# ─────────────────────────────────────────────────────────────
def _fetch_upstox_history_live(
    instrument_key: str,
    access_token: str,
    start_date: datetime,
    end_date: datetime,
    interval: str = "day",
    retries: int = 3,
):
    from_date_str = start_date.strftime("%Y-%m-%d")
    to_date_str   = end_date.strftime("%Y-%m-%d")

    # '|' must be URL-encoded as %7C
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
            resp = requests.get(url, headers=headers, timeout=20)

            if resp.status_code == 429:
                time.sleep(delay * 2)
                delay *= 2
                continue

            resp.raise_for_status()
            payload = resp.json()

            candles = payload.get("data", {}).get("candles", [])
            if not candles:
                return None

            df = pd.DataFrame(candles, columns=[
                "timestamp", "open", "high", "low", "close", "volume", "oi"
            ])

            # TIMEZONE FIX:
            # Upstox returns timestamps like "2023-01-02T00:00:00+05:30"
            # These are tz-aware. We must use tz_convert(None) to strip timezone.
            # tz_localize(None) would raise an error on tz-aware datetimes.
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            if df["timestamp"].dt.tz is not None:
                df["timestamp"] = df["timestamp"].dt.tz_convert(None)

            df.set_index("timestamp", inplace=True)
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
# SECTION C - ZERODHA PLACEHOLDER (Phase-1)
# ─────────────────────────────────────────────────────────────
def _init_zerodha_client():
    return None

def _fetch_zerodha_history(client, symbol, start_date, end_date):
    if client is None:
        return _generate_mock_ohlcv(symbol, start_date, end_date)


# ─────────────────────────────────────────────────────────────
# SECTION D - MOCK DATA GENERATOR (Zerodha fallback only)
# ─────────────────────────────────────────────────────────────
def _generate_mock_ohlcv(symbol, start_date, end_date):
    rng  = pd.date_range(start=start_date, end=end_date, freq='B')
    n    = len(rng)
    seed = abs(hash(symbol)) % (2**31)
    rs   = np.random.RandomState(seed)

    close  = 100 * np.cumprod(1 + rs.normal(0.0003, 0.015, n))
    high   = close * (1 + rs.uniform(0, 0.03, n))
    low    = close * (1 - rs.uniform(0, 0.03, n))
    open_  = close * (1 + rs.normal(0, 0.008, n))
    volume = rs.randint(50_000, 5_000_000, n).astype(float)

    return pd.DataFrame(
        {'open': open_, 'high': high, 'low': low,
         'close': close, 'volume': volume},
        index=rng
    )


# ─────────────────────────────────────────────────────────────
# SECTION E - YFINANCE FETCHER (unchanged from original)
# ─────────────────────────────────────────────────────────────
def _download_yfinance_chunk(symbols, start_date, max_retries=3, delay=2.0):
    for attempt in range(max_retries):
        try:
            return yf.download(
                symbols, start=start_date,
                progress=False, auto_adjust=True,
                threads=True, multi_level_index=False
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
        status_text.text(f"YFinance downloading... {int(progress*100)}%")
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
# SECTION F - WIDE FRAME BUILDER
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
# SECTION G - UPSTOX BULK FETCHER (LIVE)
# ─────────────────────────────────────────────────────────────
def fetch_upstox(symbols, start_date, end_date, chunk_size, progress_bar, status_text):
    # 1. Check authentication
    access_token = get_upstox_access_token(sidebar=True)
    if not access_token:
        progress_bar.progress(0.0)
        status_text.text("Waiting for Upstox login...")
        st.error("Please complete Upstox login in the sidebar, then click 'Start Data Download' again.")
        st.stop()

    # 2. Load instrument master
    instrument_map = _load_instrument_map()
    if not instrument_map:
        st.error("Could not load Upstox instrument master. Check internet connection.")
        st.stop()

    # 3. Fetch per symbol
    symbol_dfs = {}
    failed     = []
    total      = len(symbols)

    for i, sym in enumerate(symbols):
        progress = (i + 1) / total

        instrument_key = _get_instrument_key(sym, instrument_map)
        if not instrument_key:
            failed.append(sym)
            symbol_dfs[sym] = None
            continue

        try:
            df = _fetch_upstox_history_live(
                instrument_key, access_token, start_date, end_date
            )
            symbol_dfs[sym] = df
            if df is None or df.empty:
                failed.append(sym)
        except Exception as e:
            symbol_dfs[sym] = None
            failed.append(sym)

        if i % max(1, total // 20) == 0:
            fetched_so_far = (i + 1) - len(failed)
            progress_bar.progress(progress)
            status_text.text(
                f"Upstox: {int(progress*100)}% | "
                f"Fetched: {fetched_so_far} | Failed: {len(failed)}"
            )

        time.sleep(0.12)

    progress_bar.progress(1.0)
    fetched = total - len(failed)
    status_text.text(f"Upstox done - {fetched}/{total} symbols fetched.")

    close, high, volume = _build_wide_frames(symbol_dfs)

    # CRASH PREVENTION: If no data was fetched, stop cleanly
    if close.empty:
        st.error(
            "No data was fetched from Upstox. Possible reasons:\n"
            "1. Access token expired — please re-login from sidebar\n"
            "2. Instrument master did not load\n"
            "3. API rate limit hit\n\n"
            "Refresh the page, login again, and retry."
        )
        st.stop()

    return close, high, volume, failed


# ─────────────────────────────────────────────────────────────
# SECTION H - ZERODHA BULK FETCHER (Phase-1 mock)
# ─────────────────────────────────────────────────────────────
def fetch_zerodha(symbols, start_date, end_date, chunk_size, progress_bar, status_text):
    client, symbol_dfs = _init_zerodha_client(), {}
    total = len(symbols)

    for i, sym in enumerate(symbols):
        progress  = (i + 1) / total
        clean_sym = sym.replace('.NS', '')
        try:
            symbol_dfs[sym] = _fetch_zerodha_history(client, clean_sym, start_date, end_date)
        except Exception:
            symbol_dfs[sym] = None

        if i % max(1, total // 20) == 0:
            progress_bar.progress(progress)
            status_text.text(f"Zerodha downloading... {int(progress*100)}%")
        time.sleep(0.05)

    progress_bar.progress(1.0)
    status_text.text("Zerodha (MOCK) complete!")

    close, high, volume = _build_wide_frames(symbol_dfs)
    failed = [s for s, df in symbol_dfs.items() if df is None or df.empty]
    return close, high, volume, failed


# ─────────────────────────────────────────────────────────────
# SECTION I - UNIFIED ENTRY POINT
# ─────────────────────────────────────────────────────────────
def fetch_data(api_source, symbols, start_date, end_date,
               chunk_size, progress_bar, status_text) -> tuple:
    if api_source == "YFinance":
        return fetch_yfinance(symbols, start_date, chunk_size, progress_bar, status_text)
    elif api_source == "Upstox":
        return fetch_upstox(symbols, start_date, end_date, chunk_size, progress_bar, status_text)
    elif api_source == "Zerodha":
        return fetch_zerodha(symbols, start_date, end_date, chunk_size, progress_bar, status_text)
    else:
        raise ValueError(f"Unknown api_source: {api_source!r}")
