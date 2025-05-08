import time
import json
import requests
import yfinance as yf
from kafka import KafkaProducer

# Configuration
ALPHA_VANTAGE_API_KEY = "Q8RD38APCTQYWTB3"  # Replace with your key
ALPHA_VANTAGE_API_URL = "https://www.alphavantage.co/query"
KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092'
KAFKA_TOPIC = 'stocks'
STOCK_SYMBOLS = ['AAPL', 'TSLA', 'GOOG']

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(0, 10)
)

def fetch_stock_data_alpha_vantage(symbol):
    params = {
        'function': 'GLOBAL_QUOTE',
        'symbol': symbol,
        'apikey': ALPHA_VANTAGE_API_KEY
    }
    try:
        response = requests.get(ALPHA_VANTAGE_API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json().get('Global Quote')
        if data and data.get('05. price'):
            return {
                'source': 'alphavantage',
                'symbol': symbol,
                'price': data.get('05. price'),
                'volume': data.get('06. volume'),
                'timestamp': data.get('07. latest trading day') + " " + data.get('08. previous close')
            }
        else:
            print(f"Alpha Vantage: No data for {symbol} or API limit reached.")
            return None
    except Exception as e:
        print(f"Alpha Vantage Error for {symbol}: {e}")
        return None

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
        for symbol in STOCK_SYMBOLS:
            stock_data = fetch_stock_data_alpha_vantage(symbol)
            if not stock_data:
                print(f"Falling back to yfinance for {symbol}.")
                stock_data = fetch_stock_data_yfinance(symbol)

            if stock_data:
                try:
                    producer.send(KAFKA_TOPIC, value=stock_data)
                    print(f"✅ Sent to Kafka ({stock_data['source']}): {stock_data}")
                except Exception as e:
                    print(f"Kafka Error for {symbol}: {e}")

            time.sleep(13)  # Prevent Alpha Vantage rate limiting (5 reqs/min)

        print("🔁 Waiting before next data cycle...")
        time.sleep(60)  # Delay before next round
