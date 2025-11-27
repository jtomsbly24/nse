import streamlit as st
import pandas as pd
import sqlite3
import os
import requests

# ---------------- CONFIG ----------------
RAW_DB_URL = "http://152.67.7.184/db/prices.db"
LOCAL_DB = "prices.db"
TABLE_NAME = "raw_prices"

st.set_page_config(page_title="📊 NSE Strategy Scanner", layout="wide")
st.title("📊 NSE Strategy Scanner Engine (Base Layer)")

# ----------------- DB DOWNLOAD -----------------
def download_file(url, local_path):
    resp = requests.get(url, timeout=60)
    if resp.status_code == 200:
        with open(local_path, "wb") as f:
            f.write(resp.content)
        return True
    return False

@st.cache_resource
def load_db(force=False):
    if force and os.path.exists(LOCAL_DB):
        os.remove(LOCAL_DB)
    if not os.path.exists(LOCAL_DB):
        st.info("📥 Downloading DB...")
        ok = download_file(RAW_DB_URL, LOCAL_DB)
        if not ok:
            st.error("❌ Failed downloading DB")
            st.stop()
    conn = sqlite3.connect(LOCAL_DB, check_same_thread=False)
    return conn

if st.button("🔁 Force DB Refresh"):
    conn = load_db(force=True)
else:
    conn = load_db()

# ----------------- LOAD RAW PRICES -----------------
@st.cache_data
def load_raw_prices(_conn):
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", _conn)
    if TABLE_NAME not in tables["name"].values:
        st.error(f"❌ Table `{TABLE_NAME}` missing in DB.")
        st.stop()
    df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", _conn, parse_dates=["date"])
    df.sort_values(["symbol", "date"], inplace=True)
    return df

df_raw = load_raw_prices(conn)
st.success(f"✔ Loaded {len(df_raw):,} rows from `{TABLE_NAME}`.")

# ----------------- COMPUTE INDICATORS -----------------
@st.cache_data(show_spinner=True)
def compute_indicators(df):
    out = []
    for sym, g in df.groupby("symbol"):
        g = g.copy()
        g["ema20"] = g["close"].ewm(span=20).mean()
        g["ema50"] = g["close"].ewm(span=50).mean()
        g["sma150"] = g["close"].rolling(150).mean()
        g["sma200"] = g["close"].rolling(200).mean()
        g["atr14"] = (g["high"] - g["low"]).rolling(14).mean()
        g["vol_avg20"] = g["volume"].rolling(20).mean()
        g["vol_avg50"] = g["volume"].rolling(50).mean()
        g["chg_daily_pct"] = g["close"].pct_change(1) * 100
        g["chg_weekly_pct"] = g["close"].pct_change(5) * 100
        g["high5"] = g["high"].rolling(5).max()
        g["high10"] = g["high"].rolling(10).max()
        g["high20"] = g["high"].rolling(20).max()
        g["high200"] = g["high"].rolling(200).max()
        # optional RSI if column exists
        if "rsi14" not in g.columns:
            g["rsi14"] = pd.NA
        out.append(g)
    df_ind = pd.concat(out, ignore_index=True)
    return df_ind

df_ind = compute_indicators(df_raw)
st.success("✔ Indicators computed.")

# ----------------- STRATEGY FILTERS -----------------
STRATEGIES = {
    "Minervini Stage 2": [
        {"column": "ema50", "operator": ">", "value": "ema50"},  # Close > EMA50
        {"column": "sma150", "operator": ">", "value": "sma150"},  # Close > SMA150
        {"column": "sma200", "operator": ">", "value": "sma200"},  # Close > SMA200
        {"column": "close", "operator": ">=", "value": 0.8, "ref": "high200"},  # Close ≥ 80% of 200D High
        {"column": "volume", "operator": ">", "value": 1.5, "ref": "vol_avg20"},  # Volume surge
        {"column": "chg_daily_pct", "operator": ">", "value": 0},  # Daily % change
    ],
    "Qullamaggie Swing": [
        {"column": "chg_daily_pct", "operator": ">", "value": 2},  # Daily % change
        {"column": "chg_weekly_pct", "operator": ">", "value": 3},  # Weekly % change
        {"column": "close", "operator": ">", "value": "ema20"},  # Close > EMA20
        {"column": "close", "operator": ">", "value": "high5"},  # Break 5-day high
        {"column": "volume", "operator": ">", "value": 1.5, "ref": "vol_avg20"},  # Volume surge
    ],
    "StockBee Momentum Burst": [
        {"column": "chg_daily_pct", "operator": ">", "value": 3},  # Rapid daily move
        {"column": "volume", "operator": ">", "value": 2, "ref": "vol_avg50"},  # Volume spike
        {"column": "atr14", "operator": ">", "value": 0.3},  # ATR threshold
        {"column": "close", "operator": ">", "value": "high10"},  # Close > 10-day high
    ]
}

st.sidebar.header("📌 Select Strategy & Filters")
selected_strats = st.sidebar.multiselect("Select Strategy", list(STRATEGIES.keys()), default=["Minervini Stage 2"])

# Dynamically create inputs for all selected strategies
user_filters = {}
for strat in selected_strats:
    st.sidebar.markdown(f"**{strat} Filters**")
    for f in STRATEGIES[strat]:
        col = f["column"]
        op = f["operator"]
        val = f["value"]
        ref = f.get("ref", None)
        key = f"{strat}_{col}_{ref or ''}"

        if isinstance(val, (int, float)):
            user_val = st.sidebar.number_input(f"{col} {op} ?", value=float(val), step=0.1, key=key)
            user_filters[key] = user_val
        elif isinstance(val, str):
            # Column reference
            user_filters[key] = val
        elif isinstance(val, float) and ref:
            user_filters[key] = val

# ----------------- APPLY FILTERS -----------------
df_filtered = pd.DataFrame()
for strat in selected_strats:
    temp = df_ind.copy()
    for f in STRATEGIES[strat]:
        col = f["column"]
        op = f["operator"]
        val = f["value"]
        ref = f.get("ref", None)
        key = f"{strat}_{col}_{ref or ''}"

        # Determine comparison value
        if isinstance(val, (int, float)) and ref:
            comp_val = user_filters[key] * temp[ref]
        elif isinstance(val, str) and val in temp.columns:
            comp_val = temp[val]
        else:
            comp_val = user_filters[key]

        # Apply operator
        if op == ">":
            temp = temp[temp[col] > comp_val]
        elif op == ">=":
            temp = temp[temp[col] >= comp_val]
        elif op == "<":
            temp = temp[temp[col] < comp_val]
        elif op == "<=":
            temp = temp[temp[col] <= comp_val]

    df_filtered = pd.concat([df_filtered, temp])

df_filtered = df_filtered.drop_duplicates(subset=["symbol", "date"])
df_filtered = df_filtered.sort_values(["date", "symbol"], ascending=[False, True]).reset_index(drop=True)

# ----------------- OUTPUT -----------------
st.subheader("📄 Filtered Results")
st.write(f"Total symbols after filters: {df_filtered['symbol'].nunique()}")
st.dataframe(df_filtered.head(100), width='stretch')

csv = df_filtered.to_csv(index=False)
st.download_button("⬇ Download Filtered Data (CSV)", csv, "filtered_prices.csv")
