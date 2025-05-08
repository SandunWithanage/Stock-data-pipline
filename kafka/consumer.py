from kafka import KafkaConsumer
from pymongo import MongoClient
import json

consumer = KafkaConsumer('stocks',
                         bootstrap_servers='127.0.0.1:9092',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         api_version=(0, 10))

mongo = MongoClient('mongodb://localhost:27017/')
db = mongo.stock_data
realtime = db.realtime_data

for msg in consumer:
    print(f"Received: {msg.value}")
    realtime.insert_one(msg.value)
