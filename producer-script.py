# Import KafkaProducer from Kafka library
import sys
import time
from kafka import KafkaProducer
import random
import struct
from json import dumps

# Define server with port
bootstrap_servers = ['localhost:9092']

# Define topic name where the message will publish
topicName = 'test-script1'

# Initialize producer variable
producer = KafkaProducer(bootstrap_servers = bootstrap_servers,
value_serializer=lambda x: dumps(x).encode())

# generate 20 random float values between 0.0 and 200.0 and read the values into an array called data
data = [round(random.uniform(0.0, 200.0),2) for i in range(20)]
data.extend([30.00,600.00,600.00,600.00,600.00,600.00,600.00,600.00,600.00,600.00])
data.extend([55,66,77,88,99,100,101,102,103,104])

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