import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import pandas_ta as ta
import plotly.graph_objects as go
import os
import yfinance as yf
from time import sleep
from datetime import datetime

# ----------------------------
# CONFIGURATION
# ----------------------------
SUPABASE_DB_URL = os.environ.get(
    "SUPABASE_DB_URL",
    "postgresql://postgres.vlwlitpfwrtrzteouuyc:Jtomsbly837@aws-1-ap-southeast-2.pooler.supabase.com:5432/postgres"
)
TABLE_NAME = "prices"
BENCHMARK_SYMBOL = "NIFTY"
SLEEP_BETWEEN_TICKERS = 0.2

st.set_page_config(page_title="NSE Screener", layout="wide")
st.title("📈 NSE Screener — Relative Performance + RSI + ADX")

# ----------------------------
# DATABASE CONNECTION
# ----------------------------
@st.cache_resource
def get_engine():
    return create_engine(SUPABASE_DB_URL, pool_pre_ping=True)


# ----------------------------
# FUNCTIONS
# ----------------------------
@st.cache_data(ttl=600)
def load_data():
    engine = get_engine()
    query = f"SELECT symbol, date, open, high, low, close, volume FROM {TABLE_NAME};"
    df = pd.read_sql(query, engine, parse_dates=["date"])
    return df


def get_last_updated():
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT MAX(date) FROM prices;")).scalar()
        return result


def update_daily_prices():
    """Fetch latest data and update Supabase DB."""
    engine = get_engine()

    st.info("Fetching NSE tickers...")
    nse_url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    nse_df = pd.read_csv(nse_url)
    tickers = [t + ".NS" for t in nse_df["SYMBOL"].dropna().unique()]

    last_dates = {}
    with engine.connect() as conn:
        for ticker in tickers:
            symbol = ticker.split('.')[0]
            result = conn.execute(text("SELECT MAX(date) FROM prices WHERE symbol = :s"), {"s": symbol}).scalar()
            last_dates[ticker] = result

    progress_bar = st.progress(0)
    updated_count = 0

    for i, ticker in enumerate(tickers):
        symbol = ticker.split('.')[0]
        try:
            last_date = last_dates.get(ticker)
            start = None
            if last_date:
                start = (pd.to_datetime(last_date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')

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
                'date': data['date'].dt.date,
                'open': data['Open'],
                'high': data['High'],
                'low': data['Low'],
                'close': data['Close'],
                'volume': data['Volume'].fillna(0).astype(int)
            })

            df.to_sql("prices", engine, if_exists="append", index=False, method='multi')
            updated_count += len(df)
            sleep(SLEEP_BETWEEN_TICKERS)

        except Exception as e:
            st.warning(f"⚠️ {symbol}: {e}")

        progress_bar.progress((i + 1) / len(tickers))

    st.success(f"✅ Database updated successfully — {updated_count} new records added.")


# ----------------------------
# UPDATE SECTION
# ----------------------------
col1, col2 = st.columns([3, 1])
with col1:
    last_update = get_last_updated()
    if last_update:
        st.markdown(f"🕒 **Last Updated:** `{pd.to_datetime(last_update).strftime('%Y-%m-%d')}`")
    else:
        st.markdown("🕒 **Last Updated:** Not available")

with col2:
    if st.button("🔄 Update Daily Prices"):
        update_daily_prices()
        st.cache_data.clear()
        st.cache_resource.clear()
        st.experimental_rerun()

# ----------------------------
# MAIN DATA LOAD
# ----------------------------
with st.spinner("Loading data from Supabase..."):
    df = load_data()


# ----------------------------
# INDICATOR + FILTER LOGIC
# ----------------------------
def compute_indicators(df, sma_periods, ema_periods, vol_sma_period,
                       ratio_type, ratio_ma1, ratio_ma2,
                       enable_rsi, rsi_period, enable_adx, adx_period):
    df = df.sort_values(["symbol", "date"])
    results = []

    for sym, g in df.groupby("symbol", group_keys=False):
        g = g.copy()
        all_sma_periods = list(set(sma_periods + ([ratio_ma1, ratio_ma2] if ratio_type=="SMA" else [])))
        all_ema_periods = list(set(ema_periods + ([ratio_ma1, ratio_ma2] if ratio_type=="EMA" else [])))

        for p in all_sma_periods:
            g[f"sma{p}"] = g["close"].rolling(p).mean()
        for p in all_ema_periods:
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

        g[f"ratio_{ratio_type.lower()}{ratio_ma1}_{ratio_type.lower()}{ratio_ma2}"] = (
            g[col1] / g[col2] * 100
        )

        results.append(g)

    return pd.concat(results, ignore_index=True)


def compute_relative_performance(df, benchmark_symbol):
    bench = df[df["symbol"] == benchmark_symbol][["date", "close"]].rename(columns={"close": "bench_close"})
    df = df.merge(bench, on="date", how="left")
    df["relative_perf"] = (df["close"] / df["bench_close"]) * 100
    return df


def plot_stock_chart(data, symbol, sma_periods, ema_periods, enable_rsi=False, enable_adx=False):
    if data.empty:
        st.info("No data available for chart.")
        return

    fig = go.Figure()
    fig.add_trace(go.Candlestick(
        x=data["date"], open=data["open"], high=data["high"],
        low=data["low"], close=data["close"], name="Price",
        increasing_line_color="green", decreasing_line_color="red"
    ))

    for p in sma_periods:
        col = f"sma{p}"
        if col in data.columns:
            fig.add_trace(go.Scatter(x=data["date"], y=data[col],
                                     mode="lines", name=f"SMA{p}", line=dict(width=2)))
    for p in ema_periods:
        col = f"ema{p}"
        if col in data.columns:
            fig.add_trace(go.Scatter(x=data["date"], y=data[col],
                                     mode="lines", name=f"EMA{p}", line=dict(width=2, dash="dot")))

    fig.add_trace(go.Bar(
        x=data["date"], y=data["volume"], name="Volume",
        yaxis="y2", marker=dict(color="rgba(0,123,255,0.3)")
    ))

    fig.update_layout(
        title=f"{symbol} — Price, MAs & Volume",
        yaxis=dict(title="Price"),
        yaxis2=dict(title="Volume", overlaying="y", side="right", showgrid=False),
        xaxis=dict(rangeslider=dict(visible=False)),
        height=600,
        template="plotly_dark",
        legend=dict(orientation="h", y=-0.25)
    )
    st.plotly_chart(fig, use_container_width=True)
# --- RSI & ADX Subplots ---
    if enable_rsi:
        rsi_col = [c for c in data.columns if "rsi_" in c]
        if rsi_col:
            st.write("### RSI")
            st.line_chart(data.set_index("date")[rsi_col])

    if enable_adx:
        adx_cols = [c for c in ["ADX_14","DMP_14","DMN_14"] if c in data.columns]
        if adx_cols:
            st.write("### ADX")
            st.line_chart(data.set_index("date")[adx_cols])

# ----------------------------
# STREAMLIT UI
# ----------------------------
st.set_page_config(page_title="NSE Screener", layout="wide")
st.title("📈 NSE Screener — Relative Performance + RSI + ADX")

# Load Data
with st.spinner("Loading database..."):
    df = load_data()

# --- Indicator Configuration ---
st.sidebar.header("⚙️ Indicator Settings")
sma_input = st.sidebar.text_input("SMA Periods (comma separated)", "10,20,50")
ema_input = st.sidebar.text_input("EMA Periods (comma separated)", "")  # default empty
sma_periods = [int(x.strip()) for x in sma_input.split(",") if x.strip().isdigit()]
ema_periods = [int(x.strip()) for x in ema_input.split(",") if x.strip().isdigit()]
vol_sma_period = st.sidebar.number_input("Volume SMA Period", value=20, step=1)

# Ratio Settings
st.sidebar.markdown("### MA Ratio")
ratio_type = st.sidebar.radio("Type", ["SMA", "EMA"], horizontal=True)
ratio_ma1 = st.sidebar.number_input("MA1 Period", value=7, step=1)
ratio_ma2 = st.sidebar.number_input("MA2 Period", value=65, step=1)

# RSI / ADX Settings
st.sidebar.markdown("### Momentum Indicators")
enable_rsi = st.sidebar.checkbox("Enable RSI", False)
rsi_period = st.sidebar.number_input("RSI Period", value=14, step=1)
enable_adx = st.sidebar.checkbox("Enable ADX", False)
adx_period = st.sidebar.number_input("ADX Period", value=14, step=1)

# Relative Performance
st.sidebar.markdown("### Relative Performance")
enable_relative = st.sidebar.checkbox("Enable Relative Perf vs NIFTY", False)

# Compute all indicators
df = compute_indicators(df, sma_periods, ema_periods, vol_sma_period,
                        ratio_type, ratio_ma1, ratio_ma2,
                        enable_rsi, rsi_period, enable_adx, adx_period)

if enable_relative:
    df = compute_relative_performance(df, BENCHMARK_SYMBOL)

latest = df.sort_values("date").groupby("symbol").tail(1).reset_index(drop=True)

# ----------------------------
# FILTERS
# ----------------------------
st.sidebar.header("🔧 Filters")
min_price = st.sidebar.number_input("Minimum Close Price", value=80.0, step=1.0)
vol_multiplier = st.sidebar.number_input("Volume > X × Avg Volume", value=1.5, step=0.1)

with st.sidebar.expander("% Change Filters (Optional)", expanded=True):
    enable_daily = st.checkbox("Enable Daily % Change Filter", False)
    if enable_daily:
        daily_min = st.number_input("Daily % Change Min", value=-20.0)
        daily_max = st.number_input("Daily % Change Max", value=20.0)

    enable_weekly = st.checkbox("Enable Weekly % Change Filter", False)
    if enable_weekly:
        weekly_min = st.number_input("Weekly % Change Min", value=-20.0)
        weekly_max = st.number_input("Weekly % Change Max", value=20.0)

    enable_monthly = st.checkbox("Enable Monthly % Change Filter", False)
    if enable_monthly:
        monthly_min = st.number_input("Monthly % Change Min", value=-40.0)
        monthly_max = st.number_input("Monthly % Change Max", value=40.0)

# MA/EMA Conditions
st.sidebar.markdown("### MA/EMA Conditions")
ma_filters = {}
for p in sma_periods:
    ma_filters[f"sma{p}"] = st.sidebar.checkbox(f"Close > SMA{p}", True)
for p in ema_periods:
    ma_filters[f"ema{p}"] = st.sidebar.checkbox(f"Close > EMA{p}", False)
vol_surge = st.sidebar.checkbox("Volume Surge (Vol > X×AvgVol)", True)

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
st.markdown("### 📈 Stock Chart Viewer")
if not f.empty:
    symbols = f["symbol"].unique()
    selected_symbol = st.selectbox("Select Symbol to View Chart", symbols)
    if selected_symbol:
        sym_df = df[df["symbol"] == selected_symbol].sort_values("date").tail(120)
        plot_stock_chart(sym_df, selected_symbol, sma_periods, ema_periods, enable_rsi, enable_adx)
else:
    st.info("No stocks passed the filters.")

# ----------------------------
# EXPORT
# ----------------------------
csv = f.to_csv(index=False).encode("utf-8")
st.download_button("💾 Download Results as CSV", csv, "nse_screener_results.csv", "text/csv")
