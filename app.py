import streamlit as st
import pandas as pd
import sqlite3
import os

RAW_DB_URL = "http://152.67.7.184/db/prices.db"
LOCAL_DB = "prices.db"

# ------------------- DB Download Helper -------------------
def download_file(url, local_path):
    import requests
    resp = requests.get(url, timeout=30)
    if resp.status_code == 200:
        with open(local_path, "wb") as f:
            f.write(resp.content)
        return True
    return False

# ------------------- DB Loader -------------------
@st.cache_resource
def load_db(force=False):
    if force and os.path.exists(LOCAL_DB):
        os.remove(LOCAL_DB)

    if not os.path.exists(LOCAL_DB):
        st.write("Downloading DB...")
        ok = download_file(RAW_DB_URL, LOCAL_DB)
        if not ok:
            st.error("❌ Failed downloading DB")
            st.stop()

    conn = sqlite3.connect(LOCAL_DB, check_same_thread=False)
    return conn


# ------------------- Load Raw Prices -------------------
@st.cache_data
def load_raw_prices(_conn):
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", _conn)
    if "raw_prices" not in tables["name"].values:
        st.error("❌ raw_prices table missing in DB. Wrong version?")
        st.stop()

    df = pd.read_sql("SELECT * FROM raw_prices", _conn, parse_dates=["date"])
    df.sort_values(["symbol", "date"], inplace=True)
    return df


# ------------------- Indicator Engine -------------------
@st.cache_data
def compute_indicators(df):
    result = []

    for sym, d in df.groupby("symbol"):
        d = d.copy()
        d["ema50"] = d["close"].ewm(span=50).mean()
        d["sma150"] = d["close"].rolling(150).mean()
        d["sma200"] = d["close"].rolling(200).mean()
        d["atr"] = (d["high"] - d["low"]).rolling(14).mean()
        d["vol_avg20"] = d["volume"].rolling(20).mean()
        d["vol_avg50"] = d["volume"].rolling(50).mean()

        result.append(d)

    return pd.concat(result)


# ------------------- UI -------------------
st.title("📊 NSE Strategy Scanner Engine (Base Layer)")

if st.button("🔁 Force DB Refresh"):
    conn = load_db(force=True)
else:
    conn = load_db()

df_raw = load_raw_prices(conn)
st.success(f"Loaded {len(df_raw):,} rows from raw_prices.")

df_processed = compute_indicators(df_raw)

st.write("📌 Sample Processed Data Preview:")
st.dataframe(df_processed.head(50))

csv = df_processed.to_csv(index=False)
st.download_button("⬇ Download Processed Database (CSV)", csv, "processed_prices.csv")
