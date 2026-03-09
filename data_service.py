"""
data_service.py
===============
Multi-API data-fetching service for Momentum Screener.
Supports: YFinance (live) | Upstox (placeholder) | Zerodha (placeholder)

Output contract (all APIs must return this shape):
  close   : pd.DataFrame  — index=DatetimeIndex, columns=ticker symbols
  high    : pd.DataFrame  — same shape
  volume  : pd.DataFrame  — same shape  (price × volume, i.e. value in ₹)

Phase 1  → YFinance live  +  Upstox/Zerodha mock data
Phase 2  → Replace placeholder blocks with real SDK calls after credentials are added
"""

import time
import numpy as np
import pandas as pd
import yfinance as yf
import streamlit as st
from datetime import datetime

# ─────────────────────────────────────────────────────────────
# SECTION A ─ UPSTOX SDK PLACEHOLDER
# TODO (Phase-2): pip install upstox-python-sdk
#                 Replace API_KEY / SECRET / REDIRECT_URI with real values
# ─────────────────────────────────────────────────────────────
UPSTOX_CONFIG = {
    "api_key":      "YOUR_UPSTOX_API_KEY",       # ← replace in Phase-2
    "api_secret":   "YOUR_UPSTOX_API_SECRET",     # ← replace in Phase-2
    "redirect_uri": "http://localhost:8080",       # ← replace in Phase-2
    "access_token": None,                          # populated after OAuth flow
}

def _init_upstox_client():
    """
    Placeholder: initialise Upstox SDK client.
    In Phase-2, uncomment and complete the block below.
    """
    # ── Phase-2 block (uncomment when credentials are ready) ──────────────
    # import upstox_client
    # config = upstox_client.Configuration()
    # config.access_token = UPSTOX_CONFIG["access_token"]   # obtain via OAuth
    # client = upstox_client.HistoryApi(upstox_client.ApiClient(config))
    # return client
    # ─────────────────────────────────────────────────────────────────────
    return None   # Phase-1: returns None → triggers mock-data path


def _fetch_upstox_history(client, symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Fetch OHLCV for a single symbol from Upstox.
    Returns DataFrame with columns: [date, open, high, low, close, volume]

    Phase-1 returns mock data.
    Phase-2: replace body with real API call.
    """
    if client is None:
        # ── Phase-1 MOCK ────────────────────────────────────────────────
        return _generate_mock_ohlcv(symbol, start_date, end_date)

    # ── Phase-2 REAL (uncomment & adapt) ────────────────────────────────
    # interval = "1day"
    # instrument_key = f"NSE_EQ|{symbol}"   # Upstox instrument key format
    # resp = client.get_historical_candle_data1(
    #     instrument_key, interval,
    #     end_date.strftime('%Y-%m-%d'),
    #     start_date.strftime('%Y-%m-%d')
    # )
    # df = pd.DataFrame(resp.data.candles,
    #                   columns=['timestamp','open','high','low','close','volume','oi'])
    # df['date'] = pd.to_datetime(df['timestamp']).dt.date
    # return df[['date','open','high','low','close','volume']]
    # ─────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────
# SECTION B ─ ZERODHA (KITE CONNECT) SDK PLACEHOLDER
# TODO (Phase-2): pip install kiteconnect
#                 Replace API_KEY / SECRET / ACCESS_TOKEN with real values
# ─────────────────────────────────────────────────────────────
ZERODHA_CONFIG = {
    "api_key":      "YOUR_KITE_API_KEY",          # ← replace in Phase-2
    "api_secret":   "YOUR_KITE_API_SECRET",        # ← replace in Phase-2
    "access_token": None,                          # populated after login flow
}

def _init_zerodha_client():
    """
    Placeholder: initialise Kite Connect client.
    In Phase-2, uncomment and complete the block below.
    """
    # ── Phase-2 block (uncomment when credentials are ready) ──────────────
    # from kiteconnect import KiteConnect
    # kite = KiteConnect(api_key=ZERODHA_CONFIG["api_key"])
    # kite.set_access_token(ZERODHA_CONFIG["access_token"])  # obtain via kite.generate_session()
    # return kite
    # ─────────────────────────────────────────────────────────────────────
    return None   # Phase-1: returns None → triggers mock-data path


def _fetch_zerodha_history(client, symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Fetch OHLCV for a single symbol from Zerodha Kite Connect.
    Returns DataFrame with columns: [date, open, high, low, close, volume]

    Phase-1 returns mock data.
    Phase-2: replace body with real API call.
    """
    if client is None:
        # ── Phase-1 MOCK ────────────────────────────────────────────────
        return _generate_mock_ohlcv(symbol, start_date, end_date)

    # ── Phase-2 REAL (uncomment & adapt) ────────────────────────────────
    # records = client.historical_data(
    #     instrument_token = _get_zerodha_token(client, symbol),  # lookup NSE token
    #     from_date        = start_date.strftime('%Y-%m-%d'),
    #     to_date          = end_date.strftime('%Y-%m-%d'),
    #     interval         = "day"
    # )
    # df = pd.DataFrame(records)
    # df.rename(columns={'date':'date'}, inplace=True)
    # df['date'] = pd.to_datetime(df['date']).dt.date
    # return df[['date','open','high','low','close','volume']]
    # ─────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────
# SECTION C ─ MOCK DATA GENERATOR (Phase-1 only)
# ─────────────────────────────────────────────────────────────
def _generate_mock_ohlcv(symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Generates synthetic OHLCV data for Phase-1 testing.
    Shape matches the real API response contract.
    Remove / bypass in Phase-2 once real credentials are set.
    """
    rng   = pd.date_range(start=start_date, end=end_date, freq='B')  # business days
    n     = len(rng)
    seed  = abs(hash(symbol)) % (2**31)
    rs    = np.random.RandomState(seed)

    close  = 100 * np.cumprod(1 + rs.normal(0.0003, 0.015, n))
    high   = close * (1 + rs.uniform(0, 0.03, n))
    low    = close * (1 - rs.uniform(0, 0.03, n))
    open_  = close * (1 + rs.normal(0, 0.008, n))
    volume = rs.randint(50_000, 5_000_000, n).astype(float)

    return pd.DataFrame({
        'date':   rng,
        'open':   open_,
        'high':   high,
        'low':    low,
        'close':  close,
        'volume': volume,
    }).set_index('date')


# ─────────────────────────────────────────────────────────────
# SECTION D ─ YFINANCE FETCHER (existing logic, refactored)
# ─────────────────────────────────────────────────────────────
def _download_yfinance_chunk(symbols: list, start_date: datetime,
                              max_retries: int = 3, delay: float = 2.0) -> pd.DataFrame:
    """Existing YFinance chunk downloader with retry logic."""
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
    """
    Existing YFinance bulk download, split into chunks.
    Returns (close_df, high_df, volume_df) — unchanged from original.
    """
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
# SECTION E ─ UPSTOX / ZERODHA BULK FETCHER
# Converts per-symbol DataFrames → same wide-format as YFinance
# ─────────────────────────────────────────────────────────────
def _build_wide_frames(symbol_dfs: dict) -> tuple:
    """
    symbol_dfs : {ticker: DataFrame(index=DatetimeIndex, cols=[open,high,low,close,volume])}
    Returns    : (close_df, high_df, volume_df) — wide format matching YFinance output
    """
    close_map, high_map, vol_map = {}, {}, {}
    for sym, df in symbol_dfs.items():
        if df is None or df.empty:
            continue
        df.index = pd.to_datetime(df.index)
        close_map[sym] = df['close']
        high_map[sym]  = df['high']
        vol_map[sym]   = df['close'] * df['volume']   # value traded = price × volume

    close  = pd.DataFrame(close_map)
    high   = pd.DataFrame(high_map)
    volume = pd.DataFrame(vol_map)
    return close, high, volume


def fetch_upstox(symbols: list, start_date: datetime, end_date: datetime,
                 chunk_size: int, progress_bar, status_text) -> tuple:
    """
    Fetches data for all symbols via Upstox.
    Phase-1: returns mock data.
    Phase-2: replace _init_upstox_client() initialisation with real OAuth token.
    """
    client     = _init_upstox_client()
    symbol_dfs = {}
    total      = len(symbols)

    for i, sym in enumerate(symbols):
        progress = (i + 1) / total
        # Strip .NS suffix — Upstox uses plain NSE symbols
        clean_sym = sym.replace('.NS', '')
        try:
            df = _fetch_upstox_history(client, clean_sym, start_date, end_date)
            symbol_dfs[sym] = df
        except Exception as e:
            st.warning(f"Upstox: failed for {clean_sym} — {e}")
            symbol_dfs[sym] = None

        if i % max(1, total // 20) == 0:   # update every ~5%
            progress_bar.progress(progress)
            status_text.text(f"Upstox downloading… {int(progress*100)}%")
        time.sleep(0.05)  # light throttle; remove/adjust in Phase-2

    progress_bar.progress(1.0)
    status_text.text("Upstox download complete!" if client else "Upstox (MOCK) download complete!")

    close, high, volume = _build_wide_frames(symbol_dfs)
    failed = [s for s, df in symbol_dfs.items() if df is None or df.empty]
    return close, high, volume, failed


def fetch_zerodha(symbols: list, start_date: datetime, end_date: datetime,
                  chunk_size: int, progress_bar, status_text) -> tuple:
    """
    Fetches data for all symbols via Zerodha Kite Connect.
    Phase-1: returns mock data.
    Phase-2: replace _init_zerodha_client() with real session + access token.
    """
    client     = _init_zerodha_client()
    symbol_dfs = {}
    total      = len(symbols)

    for i, sym in enumerate(symbols):
        progress = (i + 1) / total
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
    status_text.text("Zerodha download complete!" if client else "Zerodha (MOCK) download complete!")

    close, high, volume = _build_wide_frames(symbol_dfs)
    failed = [s for s, df in symbol_dfs.items() if df is None or df.empty]
    return close, high, volume, failed


# ─────────────────────────────────────────────────────────────
# SECTION F ─ UNIFIED ENTRY POINT  (called from main app)
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
    status_text  : st.empty()     widget

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
        raise ValueError(f"Unknown api_source: {api_source}")
