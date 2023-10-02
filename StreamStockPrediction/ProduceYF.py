import time
from confluent_kafka import Producer
import yfinance as yf
import pandas as pd
import json
from datetime import datetime, timedelta

# Kafka broker configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092', 
    
}

# Initialize the Kafka producer
producer = Producer(kafka_config)

# Define the stock symbol you want to track
stock_symbol = 'AAPL' 

# Calculate the start time (two month ago) and end time (today)
end_time = datetime.now()
start_time = end_time - timedelta(days=60) 

while True:
    try:
        # Fetch daily stock data for the past two months
        stock_data = yf.download(stock_symbol, start=start_time, interval='1d')

        if not stock_data.empty:
            # Create a JSON message for each data point
            for _, data_point in stock_data.iterrows():
                message = {
                    'symbol': stock_symbol,
                    'date': data_point.name.strftime('%Y-%m-%d %H:%M:%S'),
                    'open': data_point['Open'],
                    'high': data_point['High'],
                    'low': data_point['Low'],
                    'close': data_point['Close'],
                    'volume': data_point['Volume']
                }

                # Send the message to the Kafka topic
                producer.produce('Stock', key=stock_symbol.encode(), value=json.dumps(message).encode('utf-8'))
                producer.flush()
                print(f"Sent message: {json.dumps(message)}")

        # Update the start time and end time for the next iteration
        start_time = end_time - timedelta(days=60)  # two month ago
        end_time = datetime.now()

    except Exception as e:
        print(f"Error fetching or producing data: {e}")

    time.sleep(1440)  # Update data each 24 hours
