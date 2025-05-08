from pymongo import MongoClient
from datetime import datetime, timedelta

client = MongoClient("mongodb://localhost:27017/")
db = client.stock_data

cutoff_time = datetime.now() - timedelta(hours=24)

# Find entries older than 24h
old_entries = list(db.realtime_data.find({
    "timestamp": {"$lt": cutoff_time.isoformat()}
}))

if old_entries:
    db.historical_data.insert_many(old_entries)
    db.realtime_data.delete_many({
        "timestamp": {"$lt": cutoff_time.isoformat()}
    })
    print(f"✅ Archived {len(old_entries)} entries from realtime_data to historical_data.")
else:
    print("ℹ️ No entries to archive.")

