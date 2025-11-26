# ------------------------------ app.py ------------------------------
import streamlit as st
import pandas as pd
import pandas_ta as ta
import requests
import sqlite3
import os
from datetime import datetime

# ------------------- CONFIG -------------------
RAW_DB_URL = "http://<YOUR_SERVER_IP>/db/prices.db"  # Update with your NGINX URL
LOCAL_DB = "prices.db"
BENCHMARK_SYMBOL = "^NSEI"

st.set_page_config(page_title="📊 NSE Screener", layout="wide")

# ------------------- STYLES -------------------
st.markdown("""
<style>
.metric-card {
    background-color: #ffffff;
    border-radius: 12px;
    box-shadow: 0 1px 5px rgba(0,0,0,0.1);
    padding: 16px;
    text-align: center;
    margin-bottom: 10px;
}
.metric-value { font-size: 1.5rem; font-weight: 600; color: #1a73e8; }
.metric-label { font-size: 0.9rem; color: #6b7280; }
</style>
""", unsafe_allow_html=True)

st.title("📊 NSE Screener Dashboard")

# ------------------- DB ACCESS -------------------
@st.cache_resource
def download_and_connect(raw_url=RAW_DB_URL, local_file=LOCAL_DB):
    if not os.path.exists(local_file):
        r = requests.get(raw_url, timeout=60)
        r.raise_for_status()
        with open(local_file, "wb") as f:
            f.write(r.content)
    conn = sqlite3.connect(local_file, check_same_thread=False)
    return conn

conn = download_and_connect()

@st.cache_data
def load_prices():
    df = pd.read_sql_query("SELECT * FROM raw_prices", conn, parse_dates=["date"])
    return df

df_prices = load_prices()

# ------------------- INDICATOR ENGINE -------------------
@st.cache_data
def compute_indicators(df, sma_periods=[20,50], ema_periods=[10,20],
                       rsi_period=14, adx_period=14, enable_relative=True):
    df = df.sort_values(["ticker","date"])
    results = []
    ma_needed = set(sma_periods + ema_periods)
    for sym, g in df.groupby("ticker", group_keys=False):
        g = g.copy()
        for p in sorted(ma_needed):
            g[f"sma{p}"] = g["close"].rolling(p).mean()
            g[f"ema{p}"] = g["close"].ewm(span=p, adjust=False).mean()
        g['chg_daily'] = g['close'].pct_change(1) * 100
        g['chg_weekly'] = g['close'].pct_change(5) * 100
        g['chg_monthly'] = g['close'].pct_change(21) * 100
        g[f"rsi{rsi_period}"] = ta.rsi(g['close'], length=rsi_period)
        adx = ta.adx(g['high'], g['low'], g['close'], length=adx_period)
        g = pd.concat([g, adx], axis=1)
        results.append(g)
    df_out = pd.concat(results, ignore_index=True)
    if enable_relative:
        bench = df_out[df_out["ticker"]==BENCHMARK_SYMBOL][["date","close"]].rename(columns={"close":"bench_close"})
        df_out = df_out.merge(bench, on="date", how="left")
        df_out["relative_perf"] = df_out["close"] / df_out["bench_close"] * 100
    return df_out

with st.spinner("⚙️ Computing indicators…"):
    df_prices = compute_indicators(df_prices)

# ------------------- FILTER SIDEBAR -------------------
st.sidebar.header("🔧 Filters")

min_price = st.sidebar.number_input("Min Close Price", value=80.0)
max_price = st.sidebar.number_input("Max Close Price", value=5000.0)
vol_multiplier = st.sidebar.number_input("Volume > × Avg Volume", value=1.5, step=0.1)

enable_daily = st.sidebar.checkbox("Enable Daily % Filter", False)
if enable_daily:
    daily_min = st.sidebar.number_input("Daily % Min", -20.0)
    daily_max = st.sidebar.number_input("Daily % Max", 20.0)

enable_rsi = st.sidebar.checkbox("Enable RSI Filter", False)
if enable_rsi:
    rsi_max = st.sidebar.number_input("RSI Max", 30.0)

enable_relative = st.sidebar.checkbox("Enable Relative Performance Filter", False)
if enable_relative:
    rel_min = st.sidebar.number_input("Relative Perf Min", 100.0)

# ------------------- APPLY FILTERS -------------------
latest = df_prices.sort_values("date").groupby("ticker").tail(1).reset_index(drop=True)
filtered = latest[(latest["close"] >= min_price) & (latest["close"] <= max_price)]

if enable_daily:
    filtered = filtered[filtered["chg_daily"].between(daily_min, daily_max)]

if enable_rsi:
    filtered = filtered[filtered[f"rsi14"] <= rsi_max]

if enable_relative:
    filtered = filtered[filtered["relative_perf"] >= rel_min]

# ------------------- METRICS -------------------
col1, col2, col3 = st.columns(3)
col1.markdown(f"<div class='metric-card'><div class='metric-value'>{len(latest):,}</div><div class='metric-label'>Total Stocks</div></div>", unsafe_allow_html=True)
col2.markdown(f"<div class='metric-card'><div class='metric-value'>{len(filtered):,}</div><div class='metric-label'>Passed Filters</div></div>", unsafe_allow_html=True)
col3.markdown(f"<div class='metric-card'><div class='metric-value'>{filtered['chg_daily'].mean():.2f}%</div><div class='metric-label'>Avg Daily % Change</div></div>", unsafe_allow_html=True)

# ------------------- TABLE -------------------
cols = ["ticker","date","open","close","volume","chg_daily","chg_weekly","chg_monthly","rsi14","relative_perf"]
st.dataframe(filtered[cols].sort_values("ticker"), use_container_width=True)

# ------------------- CSV EXPORT -------------------
csv = filtered[cols].to_csv(index=False).encode("utf-8")
st.download_button("💾 Download CSV", csv, "filtered_stocks.csv", "text/csv")
# ------------------- STRATEGY BUCKETS -------------------
st.sidebar.header("📂 Strategy Buckets Thresholds")

# Bucket A - Minervini
bucket_a_rel = st.sidebar.number_input("Minervini: Relative > ", 100.0)
bucket_a_price_ma = st.sidebar.number_input("Minervini: Min Price > ", 80.0)

# Bucket B - QullaMaggie
bucket_b_daily = st.sidebar.number_input("QullaMaggie: Daily % > ", 2.0)
bucket_b_vol = st.sidebar.number_input("QullaMaggie: Volume × Avg Volume > ", 1.5)

# Bucket C - StockBee
bucket_c_daily = st.sidebar.number_input("StockBee: Daily % > ", 5.0)
bucket_c_rsi = st.sidebar.number_input("StockBee: RSI < ", 70.0)

# ------------------- APPLY BUCKET LOGIC -------------------
buckets = {}

# Bucket A - Minervini
buckets['Minervini'] = latest[
    (latest['close'] > latest['sma150']) &
    (latest['close'] > latest['sma200']) &
    (latest['relative_perf'] >= bucket_a_rel) &
    (latest['close'] >= bucket_a_price_ma)
]

# Bucket B - QullaMaggie
buckets['QullaMaggie'] = latest[
    (latest['chg_daily'] >= bucket_b_daily) &
    (latest['volume'] > bucket_b_vol * latest['volume'].rolling(20).mean())
]

# Bucket C - StockBee
buckets['StockBee'] = latest[
    (latest['chg_daily'] >= bucket_c_daily) &
    (latest['rsi14'] <= bucket_c_rsi) &
    (latest['relative_perf'] >= 100)
]

# ------------------- DISPLAY -------------------
for name, df_bucket in buckets.items():
    st.markdown(f"### 🗂 {name} Bucket ({len(df_bucket)} stocks)")
    st.dataframe(df_bucket[cols].sort_values("ticker"), use_container_width=True)
    csv_bucket = df_bucket[cols].to_csv(index=False).encode("utf-8")
    st.download_button(f"💾 Download {name} CSV", csv_bucket, f"{name}_stocks.csv", "text/csv")
