import streamlit as st
import sqlite3
import os
import time
import requests
import pandas as pd
import pandas_ta as ta

# ---------------- CONFIG ----------------
RAW_DB_URL = "http://152.67.7.184/db/prices.db"
LOCAL_DB = "prices.db"
TABLE_NAME = "raw_prices"

st.set_page_config(page_title="NSE DB Tester", layout="wide")
st.title("📦 NSE Database Loader + Indicators")


# ----------------- SAFE DOWNLOAD -----------------
def safe_download_db(url=RAW_DB_URL, local_path=LOCAL_DB, max_retries=4, min_size=5000):
    tmp = local_path + ".tmp"

    for attempt in range(1, max_retries + 1):
        try:
            st.write(f"📥 Downloading DB (Attempt {attempt}/{max_retries})...")
            r = requests.get(url, stream=True, timeout=120)
            r.raise_for_status()

            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            size = os.path.getsize(tmp)
            if size < min_size:
                raise Exception(f"File too small ({size} bytes).")

            os.replace(tmp, local_path)
            st.success("✔ DB downloaded successfully.")
            return True

        except Exception as e:
            st.warning(f"⚠ Download failed: {e}")
            time.sleep(2)

    st.error("❌ Could not download DB after retries.")
    return False


# ----------------- DB CONNECT -----------------
@st.cache_resource
def ensure_db(local_path=LOCAL_DB, url=RAW_DB_URL):
    if not os.path.exists(local_path):
        ok = safe_download_db(url, local_path)
        if not ok:
            raise RuntimeError("DB download failed.")

    conn = sqlite3.connect(local_path, check_same_thread=False)
    return conn


# ---------------- UI ----------------
col1, col2 = st.columns([1,4])

with col1:
    if st.button("🔄 Force DB Refresh"):
        if os.path.exists(LOCAL_DB):
            os.remove(LOCAL_DB)
        safe_download_db()
        st.rerun()

with col2:
    st.caption("Database is cached locally. Click refresh to re-download from server.")


# ---------------- VALIDATE DB ----------------
try:
    conn = ensure_db()
    size = os.path.getsize(LOCAL_DB)

    st.success(f"📁 Local DB loaded — size: {size:,} bytes")

    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table'", conn
    )
    st.write("📌 Tables found in DB:", tables["name"].tolist())

    if TABLE_NAME not in tables["name"].values:
        st.error(f"❌ Expected table '{TABLE_NAME}' NOT found in database!")
        st.stop()

    df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME} LIMIT 500", conn)
    st.success(f"✔ Table `{TABLE_NAME}` loaded — rows previewing below:")
    st.dataframe(df, use_container_width=True)

except Exception as e:
    st.error(f"DB Error: {e}")


# ---------------- Load Entire DB (Cached) ----------------
st.header("📊 Database Summary")

@st.cache_data(ttl=3600, show_spinner=True)
def load_full_db(_conn):
    df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", _conn)
    
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # normalize ticker field if needed
    if "ticker" not in df.columns and "symbol" in df.columns:
        df = df.rename(columns={"symbol": "ticker"})

    return df

df = load_full_db(conn)

# --- Summary metrics ---
total_rows = len(df)
unique_tickers = df["ticker"].nunique() if "ticker" in df.columns else "N/A"
latest_date = df["date"].max().date() if "date" in df.columns else "N/A"

col1, col2, col3 = st.columns(3)
col1.metric("Total Rows", f"{total_rows:,}")
col2.metric("Tickers", f"{unique_tickers}")
col3.metric("Latest Date", f"{latest_date}")

st.write("---")
st.subheader("📄 Full Data Preview")
st.dataframe(df.head(50), use_container_width=True)


# ---------------- Indicator Section ----------------
st.header("📈 Technical Indicators")

# --- Ticker selection ---
tickers = sorted(df["ticker"].unique())
selected_ticker = st.selectbox("Select Ticker", tickers)

# Filter and sort data
ticker_df = df[df["ticker"] == selected_ticker].copy().sort_values("date")

# --- Indicator selection ---
available_indicators = ["SMA", "EMA", "RSI", "MACD", "BBANDS"]
indicator_choice = st.multiselect(
    "Select Indicators",
    available_indicators,
    default=["SMA", "RSI"]
)

# --- Cached indicator calculator ---
@st.cache_data(ttl=1800)
def compute_indicators(data, indicators):
    data = data.copy()

    if "SMA" in indicators:
        data["SMA_20"] = ta.sma(data["close"], length=20)

    if "EMA" in indicators:
        data["EMA_20"] = ta.ema(data["close"], length=20)

    if "RSI" in indicators:
        data["RSI"] = ta.rsi(data["close"], length=14)

    if "MACD" in indicators:
        macd = ta.macd(data["close"])
        data = pd.concat([data, macd], axis=1)

    if "BBANDS" in indicators:
        bb = ta.bbands(data["close"])
        data = pd.concat([data, bb], axis=1)

    return data

result_df = compute_indicators(ticker_df, indicator_choice)

st.subheader("📄 Result Data (Latest 50 rows)")
st.dataframe(result_df.tail(50), use_container_width=True)
