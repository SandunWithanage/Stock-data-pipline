FROM python:3.9-slim

WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir streamlit pymongo yfinance kafka-python

EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.enableCORS=false"]
