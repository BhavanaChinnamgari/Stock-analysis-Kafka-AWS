from kafka import KafkaProducer
import pandas as pd
from time import sleep
import json
from json import dumps,loads

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

# Test
#producer.send('stocks-test2', value={'message': 'Hello, Bhavana!'})

# Load the stock data CSV file
df = pd.read_csv("stocks_data.csv")
df.head()

while True:
    stock_file = df.sample(1).to_dict(orient="records")[0]
    producer.send('stocks-test2', value=stock_file)
    sleep(1)

producer.flush()
producer.close()
