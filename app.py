import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime

# ✅ Try to read from Streamlit Secrets first (secure)
SUPABASE_DB_URL = st.secrets.get(
    "DATABASE_URL",
    os.environ.get(
        "SUPABASE_DB_URL",
        "postgresql+psycopg2://postgres.vlwlitpfwrtrzteouuyc:Jtomsbly837@aws-1-ap-southeast-2.pooler.supabase.com:5432/postgres?sslmode=require"
    ),
)

# ✅ Create SQLAlchemy engine
engine = create_engine(SUPABASE_DB_URL, pool_pre_ping=True)

# -------------------------------
# STREAMLIT PAGE SETUP
# -------------------------------
st.set_page_config(page_title="NSE Data Viewer", layout="wide")
st.title("📊 NSE Stock Data Viewer")
st.caption("Powered by Supabase + Streamlit Cloud")

# -------------------------------
# SIDEBAR CONTROLS
# -------------------------------
st.sidebar.header("Filters")

# Fetch unique stock symbols
@st.cache_data(ttl=300)
def get_symbols():
    query = "SELECT DISTINCT symbol FROM prices ORDER BY symbol"
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df["symbol"].tolist()

symbols = get_symbols()
selected_symbol = st.sidebar.selectbox("Choose a stock symbol", symbols)

# Date range selector
@st.cache_data(ttl=300)
def get_date_range():
    query = "SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM prices"
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df.iloc[0]["min_date"], df.iloc[0]["max_date"]

min_date, max_date = get_date_range()
start_date, end_date = st.sidebar.date_input(
    "Select Date Range",
    value=(pd.to_datetime(min_date), pd.to_datetime(max_date)),
    min_value=pd.to_datetime(min_date),
    max_value=pd.to_datetime(max_date),
)

# -------------------------------
# FETCH DATA
# -------------------------------
@st.cache_data(ttl=300)
def load_data(symbol, start_date, end_date):
    query = text("""
        SELECT * FROM prices
        WHERE symbol = :symbol
          AND date BETWEEN :start AND :end
        ORDER BY date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"symbol": symbol, "start": start_date, "end": end_date})
    return df

if selected_symbol:
    df = load_data(selected_symbol, start_date, end_date)

    if not df.empty:
        st.subheader(f"{selected_symbol} – Price History")
        st.line_chart(df.set_index("date")[["close"]])

        st.dataframe(df, use_container_width=True)
    else:
        st.warning("No data available for this date range.")

# -------------------------------
# ADD NEW DATA (OPTIONAL)
# -------------------------------
st.markdown("---")
st.subheader("📥 Add / Update Data")

with st.form("add_data_form"):
    c1, c2, c3 = st.columns(3)
    symbol = c1.text_input("Symbol", value=selected_symbol)
    date = c2.date_input("Date", value=datetime.today())
    close = c3.number_input("Close Price", min_value=0.0, step=0.01)

    col1, col2, col3, col4, col5 = st.columns(5)
    open_ = col1.number_input("Open", min_value=0.0, step=0.01)
    high = col2.number_input("High", min_value=0.0, step=0.01)
    low = col3.number_input("Low", min_value=0.0, step=0.01)
    volume = col4.number_input("Volume", min_value=0, step=1)

    submitted = st.form_submit_button("Add / Update Record")

if submitted:
    new_row = pd.DataFrame([{
        "symbol": symbol,
        "date": date,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume
    }])

    try:
        # Upsert (replace if existing)
        upsert_query = text("""
            INSERT INTO prices (symbol, date, open, high, low, close, volume)
            VALUES (:symbol, :date, :open, :high, :low, :close, :volume)
            ON CONFLICT (symbol, date)
            DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """)
        with engine.begin() as conn:
            conn.execute(upsert_query, new_row.to_dict(orient="records"))
        st.success("✅ Record added/updated successfully!")
        st.cache_data.clear()  # clear cache so new data appears
    except Exception as e:
        st.error(f"❌ Error: {e}")

# -------------------------------
# FOOTER
# -------------------------------
st.markdown("---")
st.caption("© 2025 Personal NSE Data Viewer — using free Supabase + Streamlit Cloud")

