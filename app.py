# app.py — NSE Scanner Engine (Robust + Cached)
import streamlit as st
import pandas as pd
import sqlite3
import os
import time

# ---------------- CONFIG ----------------
RAW_DB_URL = "http://152.67.7.184/db/prices.db"
LOCAL_DB = "prices.db"
EXPECTED_TABLE = "raw_prices"

st.set_page_config(page_title="NSE Scanner", layout="wide")
st.title("📊 NSE Strategy Scanner Engine (Robust Loader)")

# ---------------- DB Download Helper ----------------
def download_file(url, local_path, timeout=60):
    import requests
    try:
        resp = requests.get(url, timeout=timeout, stream=True)
        resp.raise_for_status()
        tmp = local_path + ".tmp"
        with open(tmp, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        os.replace(tmp, local_path)
        return True, None
    except Exception as e:
        try:
            tmp = local_path + ".tmp"
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        return False, str(e)


# ---------------- DB Loader ----------------
@st.cache_resource
def load_db_resource(local_path=LOCAL_DB, url=RAW_DB_URL, force_download=False):
    if force_download and os.path.exists(local_path):
        try:
            os.remove(local_path)
        except Exception:
            pass
    if not os.path.exists(local_path):
        ok, err = download_file(url, local_path)
        if not ok:
            raise RuntimeError(f"Download failed: {err}")
    conn = sqlite3.connect(local_path, check_same_thread=False)
    return conn


# ---------------- Raw Prices Loader ----------------
@st.cache_data(show_spinner=False)
def load_raw_prices(_conn, table_name=EXPECTED_TABLE):
    """
    Loads price table, normalizes 'symbol', parses date.
    """
    # list tables
    tables_df = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", _conn)
    table_list = tables_df["name"].tolist()
    if table_name not in table_list:
        raise ValueError(f"Expected table '{table_name}' not found. Available tables: {table_list}")

    df = pd.read_sql(f"SELECT * FROM {table_name}", _conn)

    # --- Normalize symbol/ticker ---
    cols_lower = [c.lower() for c in df.columns]
    if "symbol" in cols_lower:
        symbol_col = [c for c in df.columns if c.lower() == "symbol"][0]
    elif "ticker" in cols_lower:
        symbol_col = [c for c in df.columns if c.lower() == "ticker"][0]
        df = df.rename(columns={symbol_col: "symbol"})
        symbol_col = "symbol"
    else:
        raise ValueError(f"Table must contain 'symbol' or 'ticker'. Found columns: {df.columns.tolist()}")

    # --- Normalize date ---
    date_col = None
    for c in df.columns:
        if c.lower() == "date":
            date_col = c
            break
    if date_col is None:
        raise ValueError("No 'date' column found in table.")
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    if date_col != "date":
        df = df.rename(columns={date_col: "date"})

    # --- Check required columns ---
    required = ["symbol", "date", "open", "high", "low", "close", "volume"]
    missing = [c for c in required if c.lower() not in [x.lower() for x in df.columns]]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # --- Sort by symbol & date ---
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    return df


# ---------------- Indicator Engine ----------------
@st.cache_data(ttl=3600, show_spinner=True)
def compute_indicators(df):
    """
    Compute indicators per symbol.
    """
    out = []
    for sym, g in df.groupby("symbol", sort=True):
        g = g.sort_values("date").reset_index(drop=True).copy()
        g["ema50"] = g["close"].ewm(span=50, adjust=False).mean()
        g["sma150"] = g["close"].rolling(150, min_periods=1).mean()
        g["sma200"] = g["close"].rolling(200, min_periods=1).mean()
        tr1 = (g["high"] - g["low"]).abs()
        tr2 = (g["high"] - g["close"].shift(1)).abs()
        tr3 = (g["low"] - g["close"].shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        g["atr14"] = tr.rolling(14, min_periods=1).mean()
        g["vol_avg20"] = g["volume"].rolling(20, min_periods=1).mean()
        g["vol_avg50"] = g["volume"].rolling(50, min_periods=1).mean()
        g["chg_daily_pct"] = g["close"].pct_change(1) * 100
        g["high20"] = g["high"].rolling(20, min_periods=1).max()
        g["low20"] = g["low"].rolling(20, min_periods=1).min()
        out.append(g)
    return pd.concat(out, ignore_index=True)


# ---------------- UI & Controls ----------------
st.sidebar.header("DB Controls")
force_refresh = st.sidebar.button("🔁 Force DB Refresh (download fresh)")

# Load DB
try:
    conn = load_db_resource(force_download=force_refresh)
except Exception as e:
    st.error(f"Failed to load DB: {e}")
    st.stop()

# Diagnostics
try:
    st.sidebar.write("Local DB size:", os.path.getsize(LOCAL_DB) if os.path.exists(LOCAL_DB) else "n/a")
    tables_df = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
    st.sidebar.write("Tables:", tables_df["name"].tolist())
except Exception as e:
    st.sidebar.warning(f"Could not list tables: {e}")

# Load raw_prices
try:
    df_raw = load_raw_prices(conn, EXPECTED_TABLE)
except Exception as e:
    st.error(f"Error loading table '{EXPECTED_TABLE}': {e}")
    st.stop()

st.success(f"Loaded raw_prices — rows: {len(df_raw):,}")
st.write("Columns and dtypes:")
st.write(df_raw.dtypes)

st.write("---")
st.subheader("Sample rows (first 10)")
st.dataframe(df_raw.head(10), use_container_width=True)

# Compute indicators
with st.spinner("Computing indicators..."):
    df_ind = compute_indicators(df_raw)
st.success("Indicators computed (cached).")

# Summary metrics
latest_date = df_ind["date"].max().date() if not df_ind.empty else "N/A"
unique_symbols = df_ind["symbol"].nunique() if "symbol" in df_ind.columns else 0
st.metric("Symbols", unique_symbols)
st.metric("Latest Date in DB", latest_date)
st.write("---")

# Latest row per symbol
latest_rows = df_ind.sort_values("date").groupby("symbol").tail(1).reset_index(drop=True)
display_cols = [
    "symbol", "date", "close", "chg_daily_pct", "ema50", "sma150", "sma200",
    "atr14", "vol_avg20", "vol_avg50", "high20", "low20"
]
display_cols = [c for c in display_cols if c in latest_rows.columns]

st.subheader("Latest snapshot (one row per symbol)")
st.dataframe(latest_rows[display_cols].sort_values("symbol").reset_index(drop=True), use_container_width=True)

# CSV export
csv_full = df_ind.to_csv(index=False).encode("utf-8")
st.download_button("⬇ Download processed dataset (CSV)", csv_full, "processed_prices.csv", "text/csv")

st.info("Next: implement your strategies (Minervini / Qullamaggie / StockBee).")
