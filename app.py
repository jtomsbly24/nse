# app.py
# ----------------------------------------------
# NSE SCREENER DASHBOARD (GitHub SQLite Version)
# ----------------------------------------------

import streamlit as st
import pandas as pd
import yfinance as yf
import pandas_ta as ta
import time
import os
import sqlite3
import requests
from io import BytesIO

# -------------------- CONFIG --------------------
# EDIT THESE: set RAW_DB_URL to the raw.githubusercontent URL of your db
# Example: "https://raw.githubusercontent.com/youruser/yourrepo/main/prices.db"
RAW_DB_URL = st.secrets.get("RAW_DB_URL", "https://raw.githubusercontent.com/jtomsbly24/nse/main/prices.db")

# GitHub Action trigger settings (used by "Trigger Update" button)
GH_OWNER = st.secrets.get("GH_OWNER", "<YOUR_USERNAME>")   # or put your username here
GH_REPO = st.secrets.get("GH_REPO", "<YOUR_REPO>")        # or your repo name
GH_WORKFLOW = st.secrets.get("GH_WORKFLOW", "update-db.yml")  # workflow file name in .github/workflows/
# GH_PAT should be stored in Streamlit secrets as GH_PAT
# e.g., in Streamlit Cloud: GH_PAT = ghp_xxx...

TABLE_NAME = "prices"
BENCHMARK_SYMBOL = "NIFTY"
SLEEP_BETWEEN_TICKERS = 0.3

st.set_page_config(page_title="📈 NSE Screener", layout="wide")

# -------------------- STYLES / HEADER --------------------
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
        .metric-value {
            font-size: 1.5rem;
            font-weight: 600;
            color: #1a73e8;
        }
        .metric-label {
            font-size: 0.9rem;
            color: #6b7280;
        }
    </style>
""", unsafe_allow_html=True)

st.title("📊 NSE Screener Dashboard")
st.markdown("""
<div style='text-align: center; margin: 15px 0 30px 0;'>
    <a href='https://chartink.com/dashboard/280745?s=35' target='_blank' rel='noreferrer'
       style='display: inline-block; background-color: #1a73e8; color: white;
              padding: 12px 28px; border-radius: 10px; text-decoration: none;
              font-weight: 600; font-size: 1.05rem; box-shadow: 0 2px 5px rgba(0,0,0,0.2);'>
        📈 Open Chartink Dashboard
    </a>
</div>
""", unsafe_allow_html=True)

# -------------------- DB DOWNLOAD & CONNECTION (cached) --------------------
@st.cache_resource
def download_and_connect(raw_url: str, local_filename: str = "prices.db"):
    """
    Download DB from GitHub raw URL to local file (only if missing),
    then return an sqlite3 connection.
    """
    # download if not present
    if not os.path.exists(local_filename):
        # show a small status message (can't use st.* inside cache_resource reliably)
        # so do a silent download; the UI above will inform when action occurs
        r = requests.get(raw_url, timeout=60)
        r.raise_for_status()
        with open(local_filename, "wb") as f:
            f.write(r.content)
    conn = sqlite3.connect(local_filename, check_same_thread=False)
    return conn

@st.cache_data(show_spinner=False)
def get_last_update_time_from_db(conn):
    try:
        q = f"SELECT MAX(date) FROM {TABLE_NAME}"
        df = pd.read_sql_query(q, conn, parse_dates=["date"])
        if not df.empty and df.iloc[0,0] is not None:
            return pd.to_datetime(df.iloc[0,0]).strftime("%Y-%m-%d")
    except Exception:
        return None
    return None

@st.cache_data(show_spinner=True)
def load_data_from_db(conn):
    """Load full table from the SQLite DB (cached)."""
    query = f"SELECT symbol, date, open, high, low, close, volume FROM {TABLE_NAME}"
    df = pd.read_sql_query(query, conn, parse_dates=["date"])
    return df

# helper to refresh cached DB file locally (call after GitHub Action completes)
def refresh_local_db(local_filename="prices.db", raw_url=RAW_DB_URL):
    if os.path.exists(local_filename):
        try:
            os.remove(local_filename)
        except Exception as e:
            st.warning(f"Could not remove cached DB file: {e}")
            return False
    # re-download and re-create connection via cache_resource by calling download
    download_and_connect.clear()
    load_data_from_db.clear()
    get_last_update_time_from_db.clear()
    # Accessing will cause fresh download and cache
    _ = download_and_connect(raw_url, local_filename)
    return True

# -------------------- Initialize DB connection --------------------
with st.spinner("📂 Downloading/connecting to DB from GitHub (cached)..."):
    try:
        conn = download_and_connect(RAW_DB_URL, local_filename="prices.db")
    except Exception as e:
        st.error(f"Failed to download DB from GitHub: {e}")
        st.stop()

# -------------------- Trigger GitHub Action (manual) --------------------
def trigger_github_workflow(owner: str, repo: str, workflow_file: str, token: str, ref: str = "main"):
    """
    Trigger a workflow_dispatch on GitHub.
    Returns: (success_bool, status_message)
    """
    url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow_file}/dispatches"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json"
    }
    payload = {"ref": ref}
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=20)
    except Exception as e:
        return False, f"Request error: {e}"
    if r.status_code == 204:
        return True, "Workflow triggered successfully."
    else:
        # return error text for debugging
        return False, f"Error {r.status_code}: {r.text}"

# -------------------- LAST UPDATE INFO + TRIGGER BUTTON --------------------
last_update = get_last_update_time_from_db(conn)
if "last_update" not in st.session_state:
    st.session_state["last_update"] = last_update

col_a, col_b = st.columns([3, 1])
with col_a:
    st.markdown(f"**🕒 Last Database Update:** {st.session_state['last_update'] or 'No data yet'}")
with col_b:
    # Show trigger + refresh controls
    if st.button("🔄 Trigger Update (GitHub Actions)", use_container_width=True):
        gh_pat = st.secrets.get("GH_PAT")
        if not gh_pat:
            st.error("GH_PAT secret not found. Add your GitHub PAT to Streamlit secrets as GH_PAT.")
        else:
            with st.spinner("Triggering GitHub workflow..."):
                ok, msg = trigger_github_workflow(GH_OWNER, GH_REPO, GH_WORKFLOW, gh_pat, ref="main")
                if ok:
                    st.success(msg + " Wait for Actions to finish and then click 'Refresh DB'.")
                else:
                    st.error(msg)

    if st.button("🔁 Refresh DB from GitHub", use_container_width=True):
        with st.spinner("Refreshing local DB cache..."):
            ok = refresh_local_db(local_filename="prices.db", raw_url=RAW_DB_URL)
            if ok:
                st.success("Local DB refreshed. Reloading data…")
                # re-create connection object after refresh
                conn = download_and_connect(RAW_DB_URL, local_filename="prices.db")
                # clear data cache
                load_data_from_db.clear()
                get_last_update_time_from_db.clear()
                st.session_state["last_update"] = get_last_update_time_from_db(conn)
                st.experimental_rerun()
            else:
                st.error("Failed to refresh local DB.")

# -------------------- LOAD DATA FROM DB --------------------
with st.spinner("📂 Loading data from local DB (cached)..."):
    try:
        df = load_data_from_db(conn)
    except Exception as e:
        st.error(f"Failed to read DB: {e}")
        st.stop()

# -------------------- INDICATORS & FILTERS (unchanged logic) --------------------
@st.cache_data(show_spinner=True)
def compute_indicators(df, sma_periods, ema_periods, vol_sma_period, ratio_type,
                       ratio_ma1, ratio_ma2, enable_rsi, rsi_period,
                       enable_adx, adx_period, enable_relative):
    df = df.sort_values(["symbol", "date"])
    results = []

    # gather MA periods needed
    ma_needed = set(sma_periods + ema_periods)
    ma_needed.update([ratio_ma1, ratio_ma2])

    for sym, g in df.groupby("symbol", group_keys=False):
        g = g.copy()
        for p in sorted(ma_needed):
            g[f"sma{p}"] = g["close"].rolling(p).mean()
            g[f"ema{p}"] = g["close"].ewm(span=p, adjust=False).mean()
        g[f"vol_sma{vol_sma_period}"] = g["volume"].rolling(vol_sma_period).mean()
        g["chg_daily"] = g["close"].pct_change(1) * 100
        g["chg_weekly"] = g["close"].pct_change(5) * 100
        g["chg_monthly"] = g["close"].pct_change(21) * 100
        if enable_rsi:
            g[f"rsi_{rsi_period}"] = ta.rsi(g["close"], length=rsi_period)
        if enable_adx:
            adx = ta.adx(g["high"], g["low"], g["close"], length=adx_period)
            g = pd.concat([g, adx], axis=1)
        if ratio_type == "SMA":
            col1, col2 = f"sma{ratio_ma1}", f"sma{ratio_ma2}"
        else:
            col1, col2 = f"ema{ratio_ma1}", f"ema{ratio_ma2}"
        if col1 in g.columns and col2 in g.columns:
            g[f"ratio_{ratio_type.lower()}{ratio_ma1}_{ratio_type.lower()}{ratio_ma2}"] = g[col1] / g[col2] * 100
        results.append(g)

    df_result = pd.concat(results, ignore_index=True)
    if enable_relative:
        bench = df_result[df_result["symbol"] == BENCHMARK_SYMBOL][["date", "close"]].rename(columns={"close": "bench_close"})
        df_result = df_result.merge(bench, on="date", how="left")
        df_result["relative_perf"] = (df_result["close"] / df_result["bench_close"]) * 100
    return df_result

# --- Sidebar Config ---
st.sidebar.header("📊 Indicator Settings")
sma_input = st.sidebar.text_input("SMA Periods", "10,20,50")
ema_input = st.sidebar.text_input("EMA Periods", "")
sma_periods = [int(x.strip()) for x in sma_input.split(",") if x.strip().isdigit()]
ema_periods = [int(x.strip()) for x in ema_input.split(",") if x.strip().isdigit()]
vol_sma_period = st.sidebar.number_input("Volume SMA Period", value=20, step=1)

ratio_type = st.sidebar.radio("MA Ratio Type", ["SMA", "EMA"], horizontal=True)
ratio_ma1 = int(st.sidebar.number_input("MA1", value=7))
ratio_ma2 = int(st.sidebar.number_input("MA2", value=65))

enable_rsi = st.sidebar.checkbox("Enable RSI", False)
rsi_period = st.sidebar.number_input("RSI Period", value=14)
enable_adx = st.sidebar.checkbox("Enable ADX", False)
adx_period = st.sidebar.number_input("ADX Period", value=14)
enable_relative = st.sidebar.checkbox("Enable Relative Perf vs NIFTY", False)

with st.spinner("⚙️ Computing indicators (one-time)..."):
    df = compute_indicators(df, sma_periods, ema_periods, vol_sma_period, ratio_type,
                            ratio_ma1, ratio_ma2, enable_rsi, rsi_period,
                            enable_adx, adx_period, enable_relative)

latest = df.sort_values("date").groupby("symbol").tail(1).reset_index(drop=True)

# ---- Add Open, Open-Close Difference, Previous Day Volume ----
prev_vol = (
    df.sort_values("date")
      .groupby("symbol")["volume"]
      .shift(1)
)

df["prev_volume"] = prev_vol
latest = df.sort_values("date").groupby("symbol").tail(1).reset_index(drop=True)
latest["open_close_diff"] = latest["open"] - latest["close"]

# -------------------- FILTERS --------------------
st.sidebar.header("🔧 Filters")

min_price = st.sidebar.number_input("Min Close Price", value=80.0)
vol_multiplier = st.sidebar.number_input("Volume > X × Avg Volume", value=1.5, step=0.1)
# ---- Volume > Fixed Number Filter ----
enable_vol_min = st.sidebar.checkbox("Enable Min Volume Filter", False)
min_volume_value = st.sidebar.number_input("Min Volume Value", value=100000, step=5000)

with st.sidebar.expander("% Change Filters", expanded=True):
    enable_daily = st.checkbox("Enable Daily % Filter", False)
    if enable_daily:
        daily_min = st.number_input("Daily % Min", value=-20.0)
        daily_max = st.number_input("Daily % Max", value=20.0)
    enable_weekly = st.checkbox("Enable Weekly % Filter", False)
    if enable_weekly:
        weekly_min = st.number_input("Weekly % Min", value=-20.0)
        weekly_max = st.number_input("Weekly % Max", value=20.0)
    enable_monthly = st.checkbox("Enable Monthly % Filter", False)
    if enable_monthly:
        monthly_min = st.number_input("Monthly % Min", value=-40.0)
        monthly_max = st.number_input("Monthly % Max", value=40.0)

ma_filters = {}
st.sidebar.markdown("### MA/EMA Conditions")
for p in sma_periods:
    ma_filters[f"sma{p}"] = st.sidebar.checkbox(f"Close > SMA{p}", False)
for p in ema_periods:
    ma_filters[f"ema{p}"] = st.sidebar.checkbox(f"Close > EMA{p}", False)
vol_surge = st.sidebar.checkbox("Volume Surge", True)

# -------------------- APPLY FILTERS --------------------
f = latest.copy()
f = f[f["close"] >= min_price]
if enable_daily:
    f = f[f["chg_daily"].between(daily_min, daily_max)]
if enable_weekly:
    f = f[f["chg_weekly"].between(weekly_min, weekly_max)]
if enable_monthly:
    f = f[f["chg_monthly"].between(monthly_min, monthly_max)]

for col, active in ma_filters.items():
    if active and col in f.columns:
        f = f[f["close"] > f[col]]

if vol_surge:
    vol_col = f"vol_sma{vol_sma_period}"
    if vol_col in f.columns:
        f = f[f["volume"] > vol_multiplier * f[vol_col]]
# ---- Apply Min Volume Filter ----
if enable_vol_min:
    f = f[f["volume"] >= min_volume_value]

# -------------------- METRICS --------------------
col1, col2, col3 = st.columns(3)
col1.markdown(f"<div class='metric-card'><div class='metric-value'>{len(latest):,}</div><div class='metric-label'>Total Stocks</div></div>", unsafe_allow_html=True)
col2.markdown(f"<div class='metric-card'><div class='metric-value'>{len(f):,}</div><div class='metric-label'>Passed Filters</div></div>", unsafe_allow_html=True)
col3.markdown(f"<div class='metric-card'><div class='metric-value'>{f['chg_daily'].mean():.2f}%</div><div class='metric-label'>Avg Daily % Change</div></div>", unsafe_allow_html=True)

# -------------------- RESULTS TABLE --------------------
st.markdown("### 📋 Filtered Results")

cols = ["symbol", "date", "open", "close", "volume", "open_close_diff", "prev_volume",
        "chg_daily", "chg_weekly", "chg_monthly"]

for p in sma_periods: cols.append(f"sma{p}")
for p in ema_periods: cols.append(f"ema{p}")
cols.append(f"vol_sma{vol_sma_period}")
ratio_col = f"ratio_{ratio_type.lower()}{ratio_ma1}_{ratio_type.lower()}{ratio_ma2}"
if ratio_col in f.columns: cols.append(ratio_col)
if enable_relative: cols.append("relative_perf")
if enable_rsi: cols.append(f"rsi_{rsi_period}")
if enable_adx and f"ADX_{adx_period}" in f.columns: cols.append(f"ADX_{adx_period}")

def highlight_ratio(val):
    if pd.isna(val): return ""
    elif val > 100: return "background-color: #d1fae5; font-weight: bold"
    elif val < 100: return "background-color: #fee2e2; font-weight: bold"
    return ""

st.dataframe(
    f[cols].sort_values("symbol").style.applymap(highlight_ratio, subset=[ratio_col] if ratio_col in f.columns else []),
    use_container_width=True
)

# -------------------- EXPORT --------------------
csv = f.to_csv(index=False).encode("utf-8")
st.download_button("💾 Download CSV", csv, "nse_screener_results.csv", "text/csv")

