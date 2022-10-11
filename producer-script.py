# Import KafkaProducer from Kafka library
import sys
import time
from kafka import KafkaProducer
import random
import struct
from json import dumps

data = []
# Define server with port
bootstrap_servers = ['localhost:9092']

# Define topic name where the message will publish
topicName = 'script-topic'

# Initialize producer variable
producer = KafkaProducer(bootstrap_servers = bootstrap_servers,
value_serializer=lambda x: dumps(x).encode())

data.extend([30.00,600.00,600.00,600.00,600.00,600.00,600.00,600.00,600.00,600.00])
data.extend([30.00,600.00,600.00,600.00,600.00,600.00,600.00,600.00,600.00,600.00])
data.extend([30.00,600.00,600.00,600.00,600.00,600.00,600.00,600.00,600.00,600.00])
data.extend([55,66,77,88,99,100,101,102,103,104])
print(data)
print('Starting to read input...')
# Publish text in defined topic
for val in data:
    try:
        producer.send(topicName,val)
        print(f"Reading {val} to consumer...")
        producer.flush()
        time.sleep(1)
    except KeyboardInterrupt:
        # Terminate the script
        print('Exiting Producer...')
        sys.exit()