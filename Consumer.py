from kafka import KafkaConsumer
from s3fs import S3FileSystem
import json
from json import dumps,loads

consumer = KafkaConsumer('stocks-test2', bootstrap_servers='localhost:9092', auto_offset_reset='earliest',
                         value_deserializer=lambda x: loads(x.decode('utf-8')),)

# for message in consumer:
#     print(f"Received message: {message.value.decode('utf-8')}")

# To send data file to s3 bucket
s3 = S3FileSystem()
for count, msg in enumerate(consumer):
    with s3.open("s3://kafka-stocks-project-s3bucket-bhavana/stock_market_{}.json".format(count), 'w') as file:
        json.dump(msg.value, file) 