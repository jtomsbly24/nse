# ----------------------------------------------
# NSE SCREENER DASHBOARD (Supabase Cached Version with Update Logic)
# ----------------------------------------------

import streamlit as st
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
import pandas_ta as ta
import time
import os

# -------------------- CONFIGURATION --------------------
SUPABASE_DB_URL = os.environ.get(
    "SUPABASE_DB_URL",
    "postgresql://postgres.vlwlitpfwrtrzteouuyc:Jtomsbly837@aws-1-ap-southeast-2.pooler.supabase.com:5432/postgres"
)
TABLE_NAME = "prices"
BENCHMARK_SYMBOL = "NIFTY"
SLEEP_BETWEEN_TICKERS = 0.3
engine = create_engine(SUPABASE_DB_URL)

st.set_page_config(page_title="📈 NSE Screener", layout="wide")

# -------------------- HEADER --------------------
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

# -------------------- DATABASE FUNCTIONS --------------------
@st.cache_data(show_spinner=False)
def get_last_update_time():
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT MAX(date) FROM {TABLE_NAME}")).scalar()
            if result:
                return pd.to_datetime(result).strftime("%Y-%m-%d")
    except Exception:
        return None
    return None

@st.cache_data(show_spinner=True)
def load_data():
    """Load full table from database (cached)"""
    query = f"SELECT symbol, date, open, high, low, close, volume FROM {TABLE_NAME}"
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn, parse_dates=["date"])
    return df

# -------------------- UPDATE DATA FUNCTION --------------------
def get_nse_tickers():
    nse_url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    nse_df = pd.read_csv(nse_url)
    return [t + ".NS" for t in nse_df["SYMBOL"].dropna().unique()]

def update_daily_prices():
    st.info("⏳ Starting daily update process...")
    progress_placeholder = st.empty()
    status_box = st.empty()

    tickers = get_nse_tickers()
    updated_count = 0
    last_fetched = None
    batch = []
    BATCH_SIZE = 100

    conn = engine.connect()

    for i, ticker in enumerate(tickers):
        try:
            symbol = str(ticker.split(".")[0])
            result = conn.execute(
                text(f"SELECT MAX(date) FROM {TABLE_NAME} WHERE symbol = :s"),
                {"s": symbol}
            ).scalar()
            last_date = pd.to_datetime(result) if result else None
            start = (last_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d') if last_date is not None else None

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

            df = data[['date', 'Open', 'High', 'Low', 'Close', 'Volume']].copy()
            df.insert(0, 'symbol', symbol)
            df.columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']

            batch.append(df)
            last_fetched = symbol
            updated_count += len(df)

            if len(batch) >= BATCH_SIZE:
                big_df = pd.concat(batch, ignore_index=True)
                big_df.to_sql(TABLE_NAME, engine, if_exists='append', index=False, method='multi')
                batch.clear()
                progress_placeholder.write(f"📊 Batch uploaded (last: **{symbol}**)")

            status_box.info(f"Last fetched: **{last_fetched}** | Total new rows: **{updated_count}**")
            time.sleep(SLEEP_BETWEEN_TICKERS)

        except Exception as e:
            progress_placeholder.warning(f"⚠️ {ticker}: {e}")
            continue

    if batch:
        big_df = pd.concat(batch, ignore_index=True)
        big_df.to_sql(TABLE_NAME, engine, if_exists='append', index=False, method='multi')
        progress_placeholder.write(f"📊 Final batch uploaded ({len(batch)} tickers)")

    conn.close()
    st.success(f"✅ Update completed — {updated_count} new rows added.")
    if last_fetched:
        st.write(f"🕒 Last ticker processed: **{last_fetched}**")
    st.session_state["last_update"] = get_last_update_time()

# -------------------- LAST UPDATE INFO + BUTTON --------------------
last_update = get_last_update_time()
if "last_update" not in st.session_state:
    st.session_state["last_update"] = last_update

st.markdown(f"**🕒 Last Database Update:** {st.session_state['last_update'] or 'No data yet'}")

if st.button("🔄 Update Daily Prices", use_container_width=True):
    update_daily_prices()

# -------------------- INDICATORS + FILTERS (UNCHANGED) --------------------
@st.cache_data(show_spinner=True)
def compute_indicators(df, sma_periods, ema_periods, vol_sma_period, ratio_type,
                       ratio_ma1, ratio_ma2, enable_rsi, rsi_period,
                       enable_adx, adx_period, enable_relative):
    df = df.sort_values(["symbol", "date"])
    results = []

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

    df = pd.concat(results, ignore_index=True)
    if enable_relative:
        bench = df[df["symbol"] == BENCHMARK_SYMBOL][["date", "close"]].rename(columns={"close": "bench_close"})
        df = df.merge(bench, on="date", how="left")
        df["relative_perf"] = (df["close"] / df["bench_close"]) * 100
    return df

# -------------------- REST OF YOUR CODE REMAINS SAME --------------------
# (Indicator settings, filters, metrics, dataframe, export — unchanged)
