import streamlit as st
import pandas as pd
from pymongo import MongoClient

st.set_page_config(layout="wide")

client = MongoClient("mongodb://mongodb:27017/")
db = client.stock_data

st.title("📈 Real-Time Stock Dashboard")

ticker = st.selectbox("Choose a Ticker", ["AAPL", "TSLA", "GOOGL"])

data = list(db.realtime_data.find({"symbol": ticker}).sort("timestamp", -1).limit(100))
df = pd.DataFrame(data)

if not df.empty:
    st.line_chart(df.set_index("timestamp")[["Close", "High", "Low"]])
    st.metric("Last Close", df["Close"].iloc[-1])
