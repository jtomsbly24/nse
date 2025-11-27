import streamlit as st
import sqlite3
import os
import time
import requests
import pandas as pd

# ---------------- CONFIG ----------------
RAW_DB_URL = "http://152.67.7.184/db/prices.db"
LOCAL_DB = "prices.db"
TABLE_NAME = "raw_prices"

st.set_page_config(page_title="NSE DB Tester", layout="wide")
st.title("📦 NSE Database Loader Test")

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
    st.write("📌 Tables found in DB:", tables)

    if TABLE_NAME not in tables["name"].values:
        st.error(f"❌ Expected table '{TABLE_NAME}' NOT found.")
        st.stop()

    df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME} LIMIT 500", conn)
    st.success(f"✔ Table `{TABLE_NAME}` loaded — rows previewing below:")

    st.dataframe(df, use_container_width=True)

except Exception as e:
    st.error(f"DB Error: {e}")

st.header("📊 Database Summary")

@st.cache_data(ttl=3600, show_spinner=True)
def load_full_db(_conn):
    df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", _conn, parse_dates=["date"])
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
st.subheader("📄 Data Preview")
st.dataframe(df.head(50), use_container_width=True)



