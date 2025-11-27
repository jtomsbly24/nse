import streamlit as st
import pandas as pd
import sqlite3
import os
import requests
import time

# ---------------- CONFIG ----------------
RAW_DB_URL = "http://152.67.7.184/db/prices.db"
LOCAL_DB = "prices.db"
TABLE_NAME = "raw_prices"

st.set_page_config(page_title="NSE Strategy Scanner", layout="wide")
st.title("📊 NSE Strategy Scanner (Base Layer)")

# ----------------- SAFE DOWNLOAD -----------------
def safe_download_db(url=RAW_DB_URL, local_path=LOCAL_DB, max_retries=4, min_size=5000):
    tmp = local_path + ".tmp"
    for attempt in range(1, max_retries + 1):
        try:
            st.info(f"📥 Downloading DB (Attempt {attempt}/{max_retries})...")
            r = requests.get(url, stream=True, timeout=120)
            r.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            if os.path.getsize(tmp) < min_size:
                raise Exception(f"File too small ({os.path.getsize(tmp)} bytes)")
            os.replace(tmp, local_path)
            st.success("✔ Database downloaded successfully.")
            return True
        except Exception as e:
            st.warning(f"⚠ Download attempt {attempt} failed: {e}")
            time.sleep(2)
    st.error("❌ Could not download DB after retries.")
    return False

# ----------------- DB CONNECT -----------------
@st.cache_resource
def load_db(force_download=False):
    if force_download and os.path.exists(LOCAL_DB):
        os.remove(LOCAL_DB)
    if not os.path.exists(LOCAL_DB):
        ok = safe_download_db()
        if not ok:
            st.error("❌ Failed to download DB.")
            st.stop()
    conn = sqlite3.connect(LOCAL_DB, check_same_thread=False)
    return conn

# ----------------- RAW DATA LOADER -----------------
@st.cache_data
def load_raw_prices(_conn):
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", _conn)
    if TABLE_NAME not in tables["name"].values:
        st.error(f"❌ Table `{TABLE_NAME}` missing in DB.")
        st.stop()

    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", _conn, parse_dates=["date"])

    # Normalize column names: prefer 'symbol'
    if "symbol" not in df.columns and "ticker" in df.columns:
        df = df.rename(columns={"ticker": "symbol"})
    elif "symbol" not in df.columns:
        st.error("❌ DB does not have 'symbol' or 'ticker' column.")
        st.stop()

    df.sort_values(["symbol", "date"], inplace=True)
    return df

# ----------------- INDICATOR ENGINE (PLACEHOLDER) -----------------
@st.cache_data
def compute_indicators(df):
    """Compute basic indicators. Extend later for strategy conditions."""
    out = []
    for sym, g in df.groupby("symbol"):
        g = g.copy()
        # Example indicators
        g["sma50"] = g["close"].rolling(50).mean()
        g["sma200"] = g["close"].rolling(200).mean()
        g["high200"] = g["high"].rolling(200).max()
        g["vol_avg20"] = g["volume"].rolling(20).mean()
        out.append(g)
    return pd.concat(out, ignore_index=True)

# ----------------- SIDEBAR -----------------
st.sidebar.header("DB / Scanner Options")
if st.sidebar.button("🔁 Force DB Refresh"):
    conn = load_db(force_download=True)
    st.rerun()
else:
    conn = load_db()

st.sidebar.subheader("Strategy Filters (Example)")
min_price = st.sidebar.number_input("Min Close Price", value=50.0, step=1.0)
enable_minervini = st.sidebar.checkbox("Minervini Stage 2", value=True)
min_minervini_pct = st.sidebar.slider("Minervini: % of 200D high", 0, 100, 80)
# Placeholder sliders / filters for other strategies
enable_qullamaggie = st.sidebar.checkbox("Qullamaggie Swing", value=False)
enable_stockbee = st.sidebar.checkbox("Stockbee Momentum", value=False)

# ----------------- LOAD RAW DB -----------------
df_raw = load_raw_prices(conn)
st.success(f"✔ Loaded {len(df_raw):,} rows from `{TABLE_NAME}`.")

# ----------------- COMPUTE INDICATORS -----------------
with st.spinner("Computing indicators…"):
    df_ind = compute_indicators(df_raw)

# ----------------- APPLY FILTERS -----------------
f = df_ind.copy()

# Min price filter
f = f[f["close"] >= min_price]

# Minervini Stage 2 filter (example: 80% of 200-day high)
if enable_minervini:
    f = f.dropna(subset=["high200"])
    f = f[f["close"] >= (min_minervini_pct / 100.0) * f["high200"]]

# ----------------- OUTPUT -----------------
st.header("📄 Filtered Results")
st.write(f"Total symbols: {f['symbol'].nunique()} | Total rows: {len(f):,}")

display_cols = ["symbol", "date", "close", "sma50", "sma200", "high200", "vol_avg20"]
display_cols = [c for c in display_cols if c in f.columns]
st.dataframe(f[display_cols].sort_values(["symbol","date"], ascending=[True,False]), width="stretch")

# CSV export
csv = f[display_cols].to_csv(index=False).encode("utf-8")
st.download_button("⬇ Download CSV", csv, "strategy_filtered.csv")

