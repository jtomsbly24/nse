# ----------------------------------------------
# NSE SCREENER DASHBOARD (Cloud DB + Strategy Buckets)
# ----------------------------------------------

import streamlit as st
import pandas as pd
import pandas_ta as ta
import requests
import os
import sqlite3
from datetime import datetime

# ---------------- CONFIG ----------------
RAW_DB_URL = "http://152.67.7.184/db/prices.db"  # Your NGINX URL
LOCAL_DB = "prices.db"
TABLE_NAME = "raw_prices"
BENCHMARK_SYMBOL = "NSEI"  # Nifty index symbol in your DB

st.set_page_config(page_title="📈 NSE Screener", layout="wide")

# ---------------- DB DOWNLOAD & CONNECTION ----------------
@st.cache_resource
def download_and_connect(url=RAW_DB_URL, local_path=LOCAL_DB):
    # Download DB if not exists
    if not os.path.exists(local_path):
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        with open(local_path, "wb") as f:
            f.write(r.content)

    conn = sqlite3.connect(local_path, check_same_thread=False)
    return conn

conn = download_and_connect()
cursor = conn.cursor()

# ---------------- CHECK TABLE ----------------
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [x[0] for x in cursor.fetchall()]
if TABLE_NAME not in tables:
    st.error(f"❌ Table '{TABLE_NAME}' does not exist in the DB.")
    st.stop()

# ---------------- LOAD DATA ----------------
@st.cache_data(show_spinner=True)
def load_data():
    df = pd.read_sql_query(
        f"SELECT * FROM {TABLE_NAME}", conn, parse_dates=["date"]
    )
    return df

df = load_data()

st.title("📊 NSE Screener Dashboard")
st.markdown(f"**🕒 Data last updated:** {df['date'].max().date()}")

# ---------------- INDICATOR ENGINE ----------------
@st.cache_data(show_spinner=True)
def compute_indicators(df):
    df = df.sort_values(["ticker", "date"])
    df["sma10"] = df["close"].rolling(10).mean()
    df["sma20"] = df["close"].rolling(20).mean()
    df["sma50"] = df["close"].rolling(50).mean()
    df["sma150"] = df["close"].rolling(150).mean()
    df["sma200"] = df["close"].rolling(200).mean()
    df["ema10"] = df["close"].ewm(span=10, adjust=False).mean()
    df["rsi14"] = ta.rsi(df["close"], length=14)
    df["chg_daily"] = df["close"].pct_change(1) * 100
    df["chg_weekly"] = df["close"].pct_change(5) * 100
    df["chg_monthly"] = df["close"].pct_change(21) * 100

    # Relative performance vs Nifty
    bench = df[df["ticker"] == BENCHMARK_SYMBOL][["date", "close"]].rename(columns={"close": "bench_close"})
    df = df.merge(bench, on="date", how="left")
    df["relative_perf"] = (df["close"] / df["bench_close"]) * 100

    return df

df = compute_indicators(df)
latest = df.sort_values("date").groupby("ticker").tail(1).reset_index(drop=True)

# ---------------- STRATEGY BUCKETS ----------------
st.sidebar.header("📂 Strategy Buckets Thresholds")

# Minervini
bucket_a_rel = st.sidebar.number_input("Minervini: Relative % >", 100.0)
bucket_a_price_ma = st.sidebar.number_input("Minervini: Min Price >", 80.0)

# QullaMaggie
bucket_b_daily = st.sidebar.number_input("QullaMaggie: Daily % >", 2.0)
bucket_b_vol = st.sidebar.number_input("QullaMaggie: Volume × Avg Volume >", 1.5)

# StockBee
bucket_c_daily = st.sidebar.number_input("StockBee: Daily % >", 5.0)
bucket_c_rsi = st.sidebar.number_input("StockBee: RSI <", 70.0)

buckets = {}

# Minervini
buckets["Minervini"] = latest[
    (latest["close"] > latest["sma150"]) &
    (latest["close"] > latest["sma200"]) &
    (latest["relative_perf"] >= bucket_a_rel) &
    (latest["close"] >= bucket_a_price_ma)
]

# QullaMaggie
buckets["QullaMaggie"] = latest[
    (latest["chg_daily"] >= bucket_b_daily) &
    (latest["volume"] > bucket_b_vol * latest["volume"].rolling(20).mean())
]

# StockBee
buckets["StockBee"] = latest[
    (latest["chg_daily"] >= bucket_c_daily) &
    (latest["rsi14"] <= bucket_c_rsi) &
    (latest["relative_perf"] >= 100)
]

# ---------------- DISPLAY ----------------
for name, df_bucket in buckets.items():
    st.markdown(f"### 🗂 {name} Bucket ({len(df_bucket)} stocks)")
    display_cols = ["ticker", "date", "open", "close", "volume", "chg_daily", "chg_weekly", "chg_monthly",
                    "sma10","sma20","sma50","sma150","sma200","ema10","rsi14","relative_perf"]
    df_display = df_bucket[display_cols].sort_values("ticker")
    st.dataframe(df_display, use_container_width=True)
    csv_bucket = df_display.to_csv(index=False).encode("utf-8")
    st.download_button(f"💾 Download {name} CSV", csv_bucket, f"{name}_stocks.csv", "text/csv")
