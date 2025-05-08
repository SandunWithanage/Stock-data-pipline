import time
import json
import yfinance as yf
from kafka import KafkaProducer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092'
KAFKA_TOPIC = 'stocks'
STOCK_SYMBOLS = ['AAPL', 'TSLA', 'GOOG']

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10)
)

def fetch_stock_data_yfinance(symbol):
    try:
        stock = yf.Ticker(symbol)
        hist = stock.history(period="1d", interval="1m")
        if hist.empty:
            print(f"yfinance: No data for {symbol}")
            return None
        latest = hist.tail(1).iloc[0]
        return {
            'source': 'yfinance',
            'symbol': symbol,
            'price': latest['Close'],
            'volume': latest['Volume'],
            'timestamp': str(hist.tail(1).index[0])
        }
    except Exception as e:
        print(f"yfinance Error for {symbol}: {e}")
        return None

if __name__ == "__main__":
    while True:
        print("⏳ Fetching stock data...")
        for symbol in STOCK_SYMBOLS:
            stock_data = fetch_stock_data_yfinance(symbol)
            if stock_data:
                try:
                    producer.send(KAFKA_TOPIC, value=stock_data)
                    print(f"✅ Sent to Kafka: {stock_data}")
                except Exception as e:
                    print(f"Kafka Error for {symbol}: {e}")
            time.sleep(10)  # Delay between symbols to avoid rate limits

        print("🔁 Waiting 60s before next cycle...")
        time.sleep(60)
