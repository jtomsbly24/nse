import yfinance as yf
import pandas as pd
import sqlite3
from tqdm import tqdm
from time import sleep
import os

DB_FILE = "prices.db"      # database file stored in repo
SLEEP_BETWEEN_TICKERS = 0.1       # small delay to avoid throttling

# Connect DB (creates if missing)
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# Ensure table exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS prices (
    symbol TEXT,
    date TEXT,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL
)
""")
conn.commit()

# Fetch NSE tickers
nse_url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
nse_df = pd.read_csv(nse_url)
tickers = [t + ".NS" for t in nse_df["SYMBOL"].dropna().unique()]

# Determine last stored date for each ticker
last_dates = {}
for ticker in tickers:
    cursor.execute("SELECT MAX(date) FROM prices WHERE symbol = ?", (ticker.split('.')[0],))
    result = cursor.fetchone()
    last_dates[ticker] = result[0] if result and result[0] else None

# Fetch and append new data
for ticker in tqdm(tickers):
    try:
        last_date = last_dates[ticker]

        # If DB has data → start from next day
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

        data.reset_index(inplace=True)
        data.rename(columns={'Date': 'date'}, inplace=True)

        df = pd.DataFrame({
            'symbol': ticker.split('.')[0],
            'date': data['date'].dt.strftime('%Y-%m-%d'),
            'open': data['Open'],
            'high': data['High'],
            'low': data['Low'],
            'close': data['Close'],
            'volume': data['Volume']
        })

        df.to_sql('prices', conn, if_exists='append', index=False)
        sleep(SLEEP_BETWEEN_TICKERS)

    except Exception as e:
        print(f"⚠️ {ticker}: {e}")

conn.close()
print("✅ Daily update completed (new rows added only).")
