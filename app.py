# ----------------------------------------------
# NSE SCREENER DASHBOARD (PostgreSQL Version)
# Full Feature Restore: % change filters, ratios, RSI/ADX, volume surge, etc.
# Last updated: 2025-11-11 18:47 IST
# ----------------------------------------------

import streamlit as st
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
import pandas_ta as ta
import plotly.graph_objects as go
import time
import os

# ----------------------------
# CONFIGURATION
# ----------------------------
SUPABASE_DB_URL = os.environ.get(
    "SUPABASE_DB_URL",
    "postgresql://postgres.vlwlitpfwrtrzteouuyc:Jtomsbly837@aws-1-ap-southeast-2.pooler.supabase.com:5432/postgres"
)
TABLE_NAME = "prices"
BENCHMARK_SYMBOL = "NIFTY"
SLEEP_BETWEEN_TICKERS = 0.3

engine = create_engine(SUPABASE_DB_URL)

st.set_page_config(page_title="NSE Screener", layout="wide")
st.title("📈 NSE Screener — Relative Performance + RSI + ADX + MA Ratios")

# ----------------------------
# FUNCTIONS
# ----------------------------
@st.cache_data(show_spinner=False)
def get_nse_tickers():
    nse_url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    nse_df = pd.read_csv(nse_url)
    return [t + ".NS" for t in nse_df["SYMBOL"].dropna().unique()]

def get_last_update_time():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT MAX(date) FROM prices")).scalar()
            if result:
                return pd.to_datetime(result).strftime("%Y-%m-%d")
    except Exception:
        return None
    return None

def load_data():
    query = f"SELECT symbol, date, open, high, low, close, volume FROM {TABLE_NAME}"
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, parse_dates=["date"])
    return df

def update_daily_prices():
    st.info("⏳ Starting daily update process...")
    progress_placeholder = st.empty()
    status_box = st.empty()

    tickers = get_nse_tickers()
    updated_count = 0
    last_fetched = None
    start_time = time.strftime("%Y-%m-%d %H:%M:%S")

    with engine.connect() as conn:
        for i, ticker in enumerate(tickers):
            try:
                symbol = ticker.split(".")[0]
                result = conn.execute(text("SELECT MAX(date) FROM prices WHERE symbol = :s"), {"s": symbol}).scalar()
                last_date = result if result else None
                start = (pd.to_datetime(last_date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d') if last_date else None

                data = yf.download(
                    ticker,
                    period="6mo" if not start else None,
                    start=start,
                    interval="1d",
                    progress=False,
                    auto_adjust=True
                )
                if data.empty:
                    continue

                if isinstance(data.columns, pd.MultiIndex):
                    data.columns = [col[0] for col in data.columns]

                data.reset_index(inplace=True)
                data.rename(columns={'Date': 'date'}, inplace=True)

                df = pd.DataFrame({
                    'symbol': symbol,
                    'date': data['date'].dt.strftime('%Y-%m-%d'),
                    'open': data['Open'],
                    'high': data['High'],
                    'low': data['Low'],
                    'close': data['Close'],
                    'volume': data['Volume']
                })

                df.to_sql(TABLE_NAME, con=conn, if_exists='append', index=False)
                updated_count += len(df)
                last_fetched = symbol

                progress_placeholder.write(f"📊 Updated **{symbol}** — {len(df)} rows added.")
                status_box.info(f"🕒 {updated_count} total rows | Last: {last_fetched}")
                time.sleep(SLEEP_BETWEEN_TICKERS)

            except Exception as e:
                progress_placeholder.warning(f"⚠️ {symbol}: {e}")
                continue

    st.success(f"✅ Update completed — {updated_count} rows added.")
    st.session_state["last_update"] = get_last_update_time()


# ----------------------------
# DISPLAY LAST DATABASE UPDATE
# ----------------------------
last_update = get_last_update_time()
if "last_update" not in st.session_state:
    st.session_state["last_update"] = last_update
st.markdown(f"**🕒 Last Database Update:** {st.session_state['last_update'] or 'No data yet'}")

# ----------------------------
# UPDATE BUTTON
# ----------------------------
with st.sidebar:
    st.header("⚙️ Controls")
    if st.button("🔄 Update Daily Prices", use_container_width=True):
        update_daily_prices()

# ----------------------------
# INDICATOR SETTINGS
# ----------------------------
st.sidebar.header("📊 Indicator Settings")
sma_input = st.sidebar.text_input("SMA Periods", "10,20,50")
ema_input = st.sidebar.text_input("EMA Periods", "")
sma_periods = [int(x.strip()) for x in sma_input.split(",") if x.strip().isdigit()]
ema_periods = [int(x.strip()) for x in ema_input.split(",") if x.strip().isdigit()]
vol_sma_period = st.sidebar.number_input("Volume SMA Period", value=20, step=1)

ratio_type = st.sidebar.radio("MA Ratio Type", ["SMA", "EMA"], horizontal=True)
ratio_ma1 = st.sidebar.number_input("MA1", value=7)
ratio_ma2 = st.sidebar.number_input("MA2", value=65)

enable_rsi = st.sidebar.checkbox("Enable RSI", False)
rsi_period = st.sidebar.number_input("RSI Period", value=14)
enable_adx = st.sidebar.checkbox("Enable ADX", False)
adx_period = st.sidebar.number_input("ADX Period", value=14)
enable_relative = st.sidebar.checkbox("Enable Relative Perf vs NIFTY", False)

# ----------------------------
# LOAD AND COMPUTE DATA
# ----------------------------
with st.spinner("📂 Loading data from database..."):
    df = load_data()

df = df.sort_values(["symbol", "date"])
results = []

for sym, g in df.groupby("symbol", group_keys=False):
    g = g.copy()
    for p in sma_periods:
        g[f"sma{p}"] = g["close"].rolling(p).mean()
    for p in ema_periods:
        g[f"ema{p}"] = g["close"].ewm(span=p, adjust=False).mean()
    g[f"vol_sma{vol_sma_period}"] = g["volume"].rolling(vol_sma_period).mean()

    # % changes
    g["chg_daily"] = g["close"].pct_change(1) * 100
    g["chg_weekly"] = g["close"].pct_change(5) * 100
    g["chg_monthly"] = g["close"].pct_change(21) * 100

    # RSI & ADX
    if enable_rsi:
        g[f"rsi_{rsi_period}"] = ta.rsi(g["close"], length=rsi_period)
    if enable_adx:
        adx = ta.adx(g["high"], g["low"], g["close"], length=adx_period)
        g = pd.concat([g, adx], axis=1)

    # Ratio
    if ratio_type == "SMA":
        col1, col2 = f"sma{ratio_ma1}", f"sma{ratio_ma2}"
    else:
        col1, col2 = f"ema{ratio_ma1}", f"ema{ratio_ma2}"
    if col1 in g and col2 in g:
        g[f"ratio_{ratio_type.lower()}{ratio_ma1}_{ratio_type.lower()}{ratio_ma2}"] = g[col1] / g[col2] * 100

    results.append(g)

df = pd.concat(results, ignore_index=True)

if enable_relative:
    bench = df[df["symbol"] == BENCHMARK_SYMBOL][["date", "close"]].rename(columns={"close": "bench_close"})
    df = df.merge(bench, on="date", how="left")
    df["relative_perf"] = (df["close"] / df["bench_close"]) * 100

latest = df.sort_values("date").groupby("symbol").tail(1).reset_index(drop=True)

# ----------------------------
# FILTERS
# ----------------------------
st.sidebar.header("🔧 Filters")
min_price = st.sidebar.number_input("Minimum Close Price", value=80.0)
vol_multiplier = st.sidebar.number_input("Volume > X × Avg Volume", value=1.5, step=0.1)

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
    ma_filters[f"sma{p}"] = st.sidebar.checkbox(f"Close > SMA{p}", True)
for p in ema_periods:
    ma_filters[f"ema{p}"] = st.sidebar.checkbox(f"Close > EMA{p}", False)
vol_surge = st.sidebar.checkbox("Volume Surge", True)

# ----------------------------
# APPLY FILTERS
# ----------------------------
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

# ----------------------------
# DISPLAY RESULTS
# ----------------------------
st.subheader(f"✅ {len(f)} Stocks Match Filters")

cols = ["symbol", "date", "close", "volume", "chg_daily", "chg_weekly", "chg_monthly"]
for p in sma_periods: cols.append(f"sma{p}")
for p in ema_periods: cols.append(f"ema{p}")
cols.append(f"vol_sma{vol_sma_period}")
ratio_col = f"ratio_{ratio_type.lower()}{ratio_ma1}_{ratio_type.lower()}{ratio_ma2}"
if ratio_col in f.columns: cols.append(ratio_col)
if enable_relative: cols.append("relative_perf")
if enable_rsi: cols.append(f"rsi_{rsi_period}")
if enable_adx and f"ADX_{adx_period}" in f.columns: cols.append(f"ADX_{adx_period}")

def highlight_ratio(val):
    if pd.isna(val):
        return ""
    elif val > 100:
        return "background-color: lightgreen; font-weight: bold"
    elif val < 100:
        return "background-color: lightcoral; font-weight: bold"
    return ""

st.dataframe(
    f[cols].sort_values("symbol").style.applymap(highlight_ratio, subset=[ratio_col] if ratio_col in f.columns else []),
    use_container_width=True
)

# ----------------------------
# CHART VIEWER
# ----------------------------
st.markdown("### 📊 Stock Chart Viewer")
if not f.empty:
    symbols = f["symbol"].unique()
    selected_symbol = st.selectbox("Select Symbol", symbols)
    if selected_symbol:
        sym_df = df[df["symbol"] == selected_symbol].sort_values("date").tail(120)
        fig = go.Figure()
        fig.add_trace(go.Candlestick(
            x=sym_df["date"], open=sym_df["open"], high=sym_df["high"],
            low=sym_df["low"], close=sym_df["close"],
            increasing_line_color="green", decreasing_line_color="red", name="Price"
        ))
        for p in sma_periods:
            col = f"sma{p}"
            if col in sym_df.columns:
                fig.add_trace(go.Scatter(x=sym_df["date"], y=sym_df[col], name=f"SMA{p}", line=dict(width=2)))
        for p in ema_periods:
            col = f"ema{p}"
            if col in sym_df.columns:
                fig.add_trace(go.Scatter(x=sym_df["date"], y=sym_df[col], name=f"EMA{p}", line=dict(width=2, dash="dot")))
        fig.update_layout(title=f"{selected_symbol} — Price, MAs & Volume", height=600, template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No stocks passed filters.")

# ----------------------------
# EXPORT
# ----------------------------
csv = f.to_csv(index=False).encode("utf-8")
st.download_button("💾 Download Results as CSV", csv, "nse_screener_results.csv", "text/csv")
