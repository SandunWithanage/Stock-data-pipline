import time
import json
import yfinance as yf
from kafka import KafkaProducer
from itertools import cycle
import requests_cache

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092'
KAFKA_TOPIC = 'stocks'
STOCK_SYMBOLS = ['AAPL', 'TSLA', 'GOOG']

# Setup Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10)
)

# Setup yfinance with request caching
session = requests_cache.CachedSession('yfinance.cache', expire_after=180)

def fetch_stock_data_yfinance(symbol):
    try:
        stock = yf.Ticker(symbol, session=session)
        hist = stock.history(period="1d", interval="1m")
        if hist.empty:
            print(f"⚠️ yfinance: No data for {symbol}")
            return None
        latest = hist.tail(1).iloc[0]
        return {
            'source': 'yfinance',
            'symbol': symbol,
            'price': round(float(latest['Close']), 2),
            'volume': int(latest['Volume']),
            'timestamp': str(hist.tail(1).index[0])
        }
    except Exception as e:
        print(f"❌ yfinance Error for {symbol}: {e}")
        return None

if __name__ == "__main__":
    symbol_cycle = cycle(STOCK_SYMBOLS)

    while True:
        symbol = next(symbol_cycle)
        print(f"\n🔍 Fetching {symbol}...")
        stock_data = fetch_stock_data_yfinance(symbol)

        if stock_data:
            try:
                producer.send(KAFKA_TOPIC, value=stock_data)
                print(f"✅ Sent to Kafka: {stock_data}")
            except Exception as e:
                print(f"❌ Kafka Error: {e}")
        else:
            print(f"🚫 No data sent for {symbol}.")

        # Wait between requests to avoid being rate-limited
        print("⏳ Sleeping 45 seconds...")
        time.sleep(45)
