"""
data_service.py
===============
Multi-API data-fetching service for Momentum Screener.
Supports: YFinance (live) | Upstox (LIVE) | Angel One (LIVE) | Zerodha (placeholder)
"""

import time
import requests
import numpy as np
import pandas as pd
import yfinance as yf
import streamlit as st
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

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
# SECTION I.5 — ANGEL ONE BULK FETCHER (LIVE)
# ─────────────────────────────────────────────────────────────
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

def _fetch_angelone_history_live(client, token: str, start_date: datetime, end_date: datetime, retries=3):
    historicParam = {
        "exchange": "NSE",
        "symboltoken": token,
        "interval": "ONE_DAY",
        "fromdate": start_date.strftime("%Y-%m-%d 09:15"),
        "todate": end_date.strftime("%Y-%m-%d 15:30")
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

def fetch_angelone(symbols, start_date, end_date, chunk_size, progress_bar, status_text):
    client = get_angelone_client(sidebar=True)
    if not client:
        progress_bar.progress(0.0)
        st.stop()
        
    status_text.text("Angel One Token Validated. Fetching Master...")
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

        time.sleep(0.35) # Rate limiting check for Angel One (max ~3 per sec)

        if i % 5 == 0 or i == total - 1:
            progress_bar.progress(progress)
            status_text.text(f"Angel One: {int(progress*100)}% | Fetched: {len(close_map)} | Failed: {len(failed)}")

    progress_bar.progress(1.0)
    status_text.text(f"Done - {len(close_map)}/{total} fetched | Not in master: {not_found}")

    all_idx = pd.bdate_range(start=start_date, end=end_date)
    close  = pd.DataFrame({s: v.reindex(all_idx) for s, v in close_map.items()}, index=all_idx)
    high   = pd.DataFrame({s: v.reindex(all_idx) for s, v in high_map.items()},  index=all_idx)
    volume = pd.DataFrame({s: v.reindex(all_idx) for s, v in vol_map.items()},   index=all_idx)

    if close.empty:
        st.error("No data fetched from Angel One. Try re-logging in and retry.")
        st.stop()

    return close, high, volume, failed

# ─────────────────────────────────────────────────────────────
# SECTION J — ZERODHA (Mock)
# ─────────────────────────────────────────────────────────────
def fetch_zerodha(symbols, start_date, end_date, chunk_size, progress_bar, status_text):
    status_text.text("Zerodha (MOCK) is not implemented yet.")
    st.stop()

# ─────────────────────────────────────────────────────────────
# SECTION K — UNIFIED ENTRY POINT
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
