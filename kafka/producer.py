import time
import json
import yfinance as yf
from kafka import KafkaProducer
import pandas as pd

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'stocks'
STOCK_SYMBOLS = ['AAPL', 'TSLA', 'GOOG']

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10)
)

def fetch_all_stocks(symbols):
    try:
        data = yf.download(
            tickers=" ".join(symbols),
            period="1d",
            interval="1m",
            group_by="ticker",
            progress=False,
            threads=False
        )
        if data.empty:
            print("⚠️ No data received (rate limit or symbols wrong).")
            return []

        results = []
        for symbol in symbols:
            try:
                latest = data[symbol].dropna().tail(1)
                if latest.empty:
                    continue
                row = latest.iloc[0]
                results.append({
                    'source': 'yfinance',
                    'symbol': symbol,
                    'price': round(float(row['Close']), 2),
                    'volume': int(row['Volume']),
                    'timestamp': str(latest.index[0])
                })
            except Exception as e:
                print(f"⚠️ Could not extract data for {symbol}: {e}")
        return results
    except Exception as e:
        print(f"❌ yfinance download error: {e}")
        return []

if __name__ == "__main__":
    while True:
        print(f"\n🔍 Fetching batch stock data: {', '.join(STOCK_SYMBOLS)}...")
        stocks = fetch_all_stocks(STOCK_SYMBOLS)

        for stock in stocks:
            try:
                producer.send(KAFKA_TOPIC, value=stock)
                print(f"✅ Sent to Kafka: {stock}")
            except Exception as e:
                print(f"❌ Kafka send error: {e}")

        print("⏳ Sleeping 90 seconds before next cycle...")
        time.sleep(90)
