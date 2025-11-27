import streamlit as st
import pandas as pd
import sqlite3
import requests
import os
import time
import altair as alt

# ---------------- CONFIG ----------------
RAW_DB_URL = "http://152.67.7.184/db/prices.db"   # Your server URL
LOCAL_DB = "prices.db"

st.set_page_config(page_title="Market DB Viewer", layout="wide")


# ---------------- UTILITY: SAFE DOWNLOAD ----------------
def safe_download_db(max_retries=5, delay=2):
    temp_file = LOCAL_DB + ".tmp"

    for attempt in range(1, max_retries + 1):
        try:
            st.info(f"📥 Downloading database ({attempt}/{max_retries})...")

            r = requests.get(RAW_DB_URL, stream=True, timeout=120)

            if r.status_code != 200:
                raise Exception(f"HTTP {r.status_code}")

            with open(temp_file, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            # sanity check
            if os.path.getsize(temp_file) < 5000:
                raise Exception("File too small — possibly corrupt.")

            os.replace(temp_file, LOCAL_DB)
            st.success("✅ Database downloaded.")
            return True

        except Exception as e:
            st.warning(f"⚠️ Download failed: {e}")
            time.sleep(delay)

    st.error("❌ Could not download DB after retries.")
    return False


# ---------------- INITIAL LOAD ----------------
st.title("📊 NSE Prices Viewer (Prototype)")

if not os.path.exists(LOCAL_DB):
    st.warning("Database not found. Downloading...")
    safe_download_db()

if st.button("🔄 Refresh Database"):
    safe_download_db()
    st.rerun()



# ---------------- LOAD DB ----------------
try:
    conn = sqlite3.connect(LOCAL_DB)
    df = pd.read_sql("SELECT * FROM raw_prices", conn)
    conn.close()
except Exception as e:
    st.error(f"❌ Failed to read database: {e}")
    st.stop()

# Convert date to datetime
df['date'] = pd.to_datetime(df['date'])


# ---------------- FILTER UI ----------------
tickers = sorted(df["ticker"].unique())
selected_ticker = st.sidebar.selectbox("📌 Select Ticker", tickers)

date_min = df['date'].min()
date_max = df['date'].max()

date_range = st.sidebar.date_input("📅 Select Date Range", [date_min, date_max])

df_filtered = df[
    (df['ticker'] == selected_ticker) &
    (df['date'].between(pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])))
]

# ---------------- OUTPUT ----------------
st.subheader(f"📈 {selected_ticker} — {len(df_filtered)} rows")

col1, col2, col3 = st.columns(3)
col1.metric("Start Date", str(df_filtered['date'].min().date()))
col2.metric("End Date", str(df_filtered['date'].max().date()))
col3.metric("Avg Volume", f"{df_filtered['volume'].mean():,.0f}")


# ---------------- TABLE ----------------
st.dataframe(df_filtered.sort_values("date"), use_container_width=True)


# ---------------- CHART ----------------
if not df_filtered.empty:
    chart = (
        alt.Chart(df_filtered)
        .mark_line()
        .encode(
            x="date:T",
            y="close:Q",
            tooltip=["date", "open", "high", "low", "close", "volume"]
        )
        .interactive()
    )
    st.altair_chart(chart, use_container_width=True)


st.info("🧪 This is an initial prototype. More analytics & UI coming next.")

