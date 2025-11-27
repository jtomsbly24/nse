# app.py — Robust DB loader + indicator engine (single-file)
import streamlit as st
import pandas as pd
import sqlite3
import os
import time

# NOTE: requests is imported inside download_file to avoid errors when running in restricted envs
RAW_DB_URL = "http://152.67.7.184/db/prices.db"
LOCAL_DB = "prices.db"
EXPECTED_TABLE = "raw_prices"

st.set_page_config(page_title="NSE Scanner (robust)", layout="wide")
st.title("📊 NSE Strategy Scanner Engine (Robust Loader)")

# ------------------- DB Download Helper -------------------
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
        # remove tmp if present
        try:
            tmp = local_path + ".tmp"
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        return False, str(e)


# ------------------- DB Loader -------------------
@st.cache_resource
def load_db_resource(local_path=LOCAL_DB, url=RAW_DB_URL, force_download=False):
    """
    Returns sqlite3.Connection. Use 'force_download=True' to re-download the DB.
    """
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


# ------------------- Load Raw Prices -------------------
@st.cache_data(show_spinner=False)
def load_raw_prices(_conn, table_name=EXPECTED_TABLE):
    """
    Loads the price table, normalizes column name to 'symbol', parses date.
    _conn must be named with leading underscore to avoid hashing error in Streamlit.
    """
    # list tables
    tables_df = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", _conn)
    table_list = tables_df["name"].tolist()

    if table_name not in table_list:
        raise ValueError(f"Expected table '{table_name}' not found. Available tables: {table_list}")

    # read table
    df = pd.read_sql(f"SELECT * FROM {table_name}", _conn)

    # Normalize column names
    cols_lower = [c.lower() for c in df.columns]
    # map 'ticker' -> 'symbol' if necessary
    if "symbol" not in cols_lower and "ticker" in cols_lower:
        # rename the actual column to 'symbol'
        real_col = [c for c in df.columns if c.lower() == "ticker"][0]
        df = df.rename(columns={real_col: "symbol"})
    elif "symbol" not in cols_lower and "ticker" not in cols_lower:
        raise ValueError("Neither 'symbol' nor 'ticker' column found in table. Found columns: " + ", ".join(df.columns))

    # ensure date column exists and is parsed
    date_col = None
    for c in df.columns:
        if c.lower() == "date":
            date_col = c
            break
    if date_col is None:
        raise ValueError("No 'date' column found in table.")
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # unify date column name to 'date' if case differs
    if date_col != "date":
        df = df.rename(columns={date_col: "date"})

    # standard required columns check
    required = {"symbol", "date", "open", "high", "low", "close", "volume"}
    if not required.issubset(set([c.lower() for c in df.columns])):
        missing = required - set([c.lower() for c in df.columns])
        raise ValueError(f"Missing required columns in table: {missing}")

    # sort and return
    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    return df


# ------------------- Indicator Engine -------------------
@st.cache_data(ttl=3600, show_spinner=False)
def compute_indicators(df):
    """
    Compute a set of core indicators for all tickers.
    Returns a DataFrame with additional columns added.
    """
    out = []
    # choose the column name for symbol (we normalized to 'symbol')
    group_col = "symbol"

    # iterate groupwise
    for sym, g in df.groupby(group_col, sort=True):
        g = g.sort_values("date").reset_index(drop=True).copy()

        # basic moving averages / EMAs
        g["ema50"] = g["close"].ewm(span=50, adjust=False).mean()
        g["sma150"] = g["close"].rolling(window=150, min_periods=1).mean()
        g["sma200"] = g["close"].rolling(window=200, min_periods=1).mean()

        # ATR (simple true range rolling avg)
        tr1 = (g["high"] - g["low"]).abs()
        tr2 = (g["high"] - g["close"].shift(1)).abs()
        tr3 = (g["low"] - g["close"].shift(1)).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        g["atr14"] = tr.rolling(window=14, min_periods=1).mean()

        # volume averages
        g["vol_avg20"] = g["volume"].rolling(window=20, min_periods=1).mean()
        g["vol_avg50"] = g["volume"].rolling(window=50, min_periods=1).mean()

        # pct change today
        g["chg_daily_pct"] = g["close"].pct_change(1) * 100

        # 20-day high / low (for breakout detection)
        g["high20"] = g["high"].rolling(window=20, min_periods=1).max()
        g["low20"] = g["low"].rolling(window=20, min_periods=1).min()

        out.append(g)

    df_ind = pd.concat(out, ignore_index=True)
    return df_ind


# ---------------------- UI & Controls ----------------------
st.sidebar.header("DB Controls")
force_refresh = st.sidebar.button("🔁 Force DB Refresh (download fresh)")

# Load DB resource (connection)
try:
    conn = load_db_resource(force_download=force_refresh)
except Exception as e:
    st.error(f"Failed to load DB: {e}")
    st.stop()

# Show diagnostics
try:
    st.sidebar.write("Local DB size:", os.path.getsize(LOCAL_DB) if os.path.exists(LOCAL_DB) else "n/a")
    tables_df = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
    st.sidebar.write("Tables:", tables_df["name"].tolist())
except Exception as e:
    st.sidebar.warning(f"Could not list tables: {e}")

# Load raw prices (cached)
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

# Compute indicators (cached)
with st.spinner("Computing indicators (cached)..."):
    df_ind = compute_indicators(df_raw)

st.success("Indicators computed (cached).")

# Summary metrics
latest_date = df_ind["date"].max().date() if not df_ind.empty else "N/A"
unique_symbols = df_ind["symbol"].nunique() if "symbol" in df_ind.columns else 0
st.metric("Symbols", unique_symbols)
st.metric("Latest Date in DB", latest_date)
st.write("---")

# Show sample processed data (latest row per symbol)
latest_rows = df_ind.sort_values("date").groupby("symbol").tail(1).reset_index(drop=True)
display_cols = [
    "symbol", "date", "close", "chg_daily_pct", "ema50", "sma150", "sma200",
    "atr14", "vol_avg20", "vol_avg50", "high20", "low20"
]
display_cols = [c for c in display_cols if c in latest_rows.columns]

st.subheader("Latest snapshot (one row per symbol)")
st.dataframe(latest_rows[display_cols].sort_values("symbol").reset_index(drop=True), use_container_width=True)

# Export full processed table
csv_full = df_ind.to_csv(index=False).encode("utf-8")
st.download_button("⬇ Download processed dataset (CSV)", csv_full, "processed_prices.csv", "text/csv")

st.info("Next: implement strategy rules (StockBee / Qullamaggie / Minervini).")
