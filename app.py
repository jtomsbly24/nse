# app.py
# Minimal NSE Screener (EOD) — computes indicators live, sorts by Relative Strength
import streamlit as st
import pandas as pd
import pandas_ta as ta
import requests
import sqlite3
import os
import time
from datetime import datetime

# ---------------- CONFIG ----------------
RAW_DB_URL = "http://152.67.7.184/db/prices.db"   # <-- update if different
LOCAL_DB = "prices.db"
TABLE_NAME = "raw_prices"

st.set_page_config(page_title="NSE Screener (Minimal)", layout="wide")
st.title("📈 NSE Screener — Minimal (Sorted by Relative Strength)")

# ----------------- SAFE DOWNLOAD -----------------
def safe_download_db(url=RAW_DB_URL, local_path=LOCAL_DB, max_retries=4, min_size=5000):
    """
    Download the DB to a temp file and atomically replace the local DB.
    Returns True on success, False otherwise.
    """
    tmp = local_path + ".tmp"
    for attempt in range(1, max_retries + 1):
        try:
            st.info(f"Downloading DB (attempt {attempt}/{max_retries})...")
            r = requests.get(url, stream=True, timeout=90)
            r.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            size = os.path.getsize(tmp)
            if size < min_size:
                raise Exception(f"downloaded file too small ({size} bytes)")
            # atomic replace
            os.replace(tmp, local_path)
            st.success("Database downloaded successfully.")
            return True
        except Exception as e:
            st.warning(f"Download attempt {attempt} failed: {e}")
            time.sleep(2)
    st.error("Failed to download DB after retries.")
    return False

# ----------------- DB CONNECT / VERIFY -----------------
@st.cache_resource
def ensure_db(local_path=LOCAL_DB, url=RAW_DB_URL):
    """
    Ensure local DB exists (download once). Return sqlite3.Connection object.
    """
    if not os.path.exists(local_path):
        ok = safe_download_db(url, local_path)
        if not ok:
            raise RuntimeError("Could not download DB. Check RAW_DB_URL / server.")
    conn = sqlite3.connect(local_path, check_same_thread=False)
    return conn

# allow manual refresh
col_refresh, col_info = st.columns([1, 4])
with col_refresh:
    if st.button("🔄 Refresh DB (force)"):
        ok = safe_download_db()
        if ok:
            st.rerun()

with col_info:
    st.caption("DB is cached locally; use Refresh to force re-download from server.")

# Connect and validate table
try:
    conn = ensure_db()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cursor.fetchall()]
    if TABLE_NAME not in tables:
        st.error(f"Table '{TABLE_NAME}' not found in DB. Check database contents.")
        st.stop()
except Exception as e:
    st.error(f"DB connection error: {e}")
    st.stop()

# ----------------- LOAD RAW PRICES -----------------
@st.cache_data(ttl=3600)  # cache for 1 hour; adjust as needed
def load_raw_prices(conn_obj):
    df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", conn_obj, parse_dates=["date"])
    # normalize ticker column name (some DBs use symbol/ticker)
    if "ticker" not in df.columns and "symbol" in df.columns:
        df = df.rename(columns={"symbol": "ticker"})
    return df

df_raw = load_raw_prices(conn)

st.markdown(f"**DB snapshot:** `{LOCAL_DB}` — rows: {len(df_raw):,} — last date: {df_raw['date'].max().date()}")

# ----------------- INDICATOR COMPUTE (live) -----------------
@st.cache_data(show_spinner=True)
def compute_indicators(df):
    # expects df with columns: ticker, date, open, high, low, close, volume
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # compute grouped indicators
    out = []
    for sym, g in df.groupby("ticker", group_keys=False):
        g = g.copy()
        # moving averages
        g["sma20"] = g["close"].rolling(20).mean()
        g["sma50"] = g["close"].rolling(50).mean()
        g["sma150"] = g["close"].rolling(150).mean()
        g["sma200"] = g["close"].rolling(200).mean()
        # RSI 14
        g["rsi14"] = ta.rsi(g["close"], length=14)
        # 20-day avg volume
        g["vol20"] = g["volume"].rolling(20).mean()
        # pct changes
        g["chg_daily"] = g["close"].pct_change(1) * 100
        g["chg_weekly"] = g["close"].pct_change(5) * 100
        g["chg_monthly"] = g["close"].pct_change(21) * 100
        # 20-day high for breakout detection
        g["high20"] = g["high"].rolling(20).max()
        out.append(g)
    df_ind = pd.concat(out, ignore_index=True)

    # compute relative strength vs available benchmark tickers
    # detect common Nifty tickers in DB
    bench_candidates = [t for t in df_ind["ticker"].unique() if any(x in t.upper() for x in ("^NSEI", "NSEI", "NIFTY", "NIFTY50"))]
    bench = None
    if len(bench_candidates) > 0:
        bench_name = bench_candidates[0]
        bench = df_ind[df_ind["ticker"] == bench_name][["date", "close"]].rename(columns={"close": "bench_close"})
        df_ind = df_ind.merge(bench, on="date", how="left")
        df_ind["relative_perf"] = (df_ind["close"] / df_ind["bench_close"]) * 100
    else:
        # cannot compute RS; fill NaN
        df_ind["relative_perf"] = pd.NA

    return df_ind

with st.spinner("Computing indicators (may take a moment for many tickers)…"):
    df_ind = compute_indicators(df_raw)

# latest row per ticker
latest = df_ind.sort_values("date").groupby("ticker").tail(1).reset_index(drop=True)

# ----------------- SIDEBAR FILTERS (minimal) -----------------
st.sidebar.header("Filters — Base Universe")

min_price = st.sidebar.number_input("Min Close Price (₹)", value=50.0, step=1.0)
min_volume = st.sidebar.number_input("Min Avg Volume (20d)", value=10000, step=1000)
vol_surge = st.sidebar.number_input("Volume > × Avg (multiplier)", value=1.5, step=0.1)
close_above_sma50 = st.sidebar.checkbox("Close > SMA50", value=False)
close_above_sma200 = st.sidebar.checkbox("Close > SMA200", value=False)
enable_rsi = st.sidebar.checkbox("Enable RSI filter", value=False)
if enable_rsi:
    rsi_low = st.sidebar.number_input("RSI min", value=0, step=1)
    rsi_high = st.sidebar.number_input("RSI max", value=70, step=1)
enable_breakout = st.sidebar.checkbox("20-day high breakout (Close > 20D high)", value=False)

# quick apply button
if st.sidebar.button("Apply Filters"):
    st.experimental_rerun()

# ----------------- APPLY FILTERS -----------------
f = latest.copy()

# basic price and volume
f = f[f["close"] >= min_price]

# ensure vol20 exists (drop NaN)
f = f.dropna(subset=["vol20"])

f = f[f["vol20"] >= min_volume]

# volume surge
f = f[f["volume"] > vol_surge * f["vol20"]]

# MA filters
if close_above_sma50 and "sma50" in f.columns:
    f = f[f["close"] > f["sma50"]]
if close_above_sma200 and "sma200" in f.columns:
    f = f[f["close"] > f["sma200"]]

# RSI filter
if enable_rsi:
    f = f.dropna(subset=["rsi14"])
    f = f[(f["rsi14"] >= rsi_low) & (f["rsi14"] <= rsi_high)]

# breakout filter
if enable_breakout:
    f = f.dropna(subset=["high20"])
    f = f[f["close"] > f["high20"]]

# ----------------- SORT (Relative Strength preferred) -----------------
# If relative_perf is available, sort by it desc; otherwise sort by chg_daily desc
if "relative_perf" in f.columns and f["relative_perf"].notna().sum() > 0:
    f = f.sort_values("relative_perf", ascending=False)
else:
    f = f.sort_values("chg_daily", ascending=False)

# ----------------- OUTPUT METRICS -----------------
c1, c2, c3 = st.columns(3)
c1.metric("Universe (unique tickers)", len(df_ind["ticker"].unique()))
c2.metric("Candidates after filters", len(f))
avg_chg = f["chg_daily"].mean() if not f.empty else 0.0
c3.metric("Avg Daily % (filtered)", f"{avg_chg:.2f}%")

st.markdown("---")
st.subheader("Results (latest row per ticker)")

display_cols = ["ticker", "date", "close", "chg_daily", "chg_weekly", "chg_monthly", "volume", "vol20", "sma20", "sma50", "sma150", "sma200", "rsi14", "relative_perf"]
display_cols = [c for c in display_cols if c in f.columns]

st.dataframe(f[display_cols].reset_index(drop=True), use_container_width=True)

# CSV export
csv = f[display_cols].to_csv(index=False).encode("utf-8")
st.download_button("💾 Download CSV (filtered)", csv, "filtered_nse.csv", "text/csv")

st.info("Minimal UI + filters active. Sort order: Relative Strength (descending) when available.")

