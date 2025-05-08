import yfinance as yf
import pandas as pd
from pymongo import MongoClient
from time import sleep
from datetime import datetime

client = MongoClient("mongodb://localhost:27017/")
db = client.stock_data

symbols = ['AAPL', 'TSLA', 'GOOG']

for symbol in symbols:
    print(f"📥 Downloading historical data for {symbol} using yfinance...")
    try:
        data = yf.download(symbol, period="1y", interval="1d", progress=False)
        if data.empty:
            print(f"⚠️ No data returned for {symbol}")
            continue

        data.reset_index(inplace=True)
        inserted_count = 0

        for row in data.to_dict("records"):
            if "Date" not in row or pd.isna(row["Date"]):
                continue

            doc = {
                "symbol": symbol,
                "timestamp": row["Date"].isoformat() if isinstance(row["Date"], datetime) else str(row["Date"]),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "adj_close": float(row["Adj Close"]),
                "volume": int(row["Volume"]),
                "source": "yfinance"
            }
            db.historical_data.insert_one(doc)
            inserted_count += 1

        print(f"✅ Inserted {inserted_count} records for {symbol}")
        sleep(5)  # avoid rate limit

    except Exception as e:
        print(f"❌ Failed for {symbol}: {e}")
