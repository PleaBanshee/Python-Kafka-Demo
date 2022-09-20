# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer
from pymongo import MongoClient
import json
# Import sys module
import sys
import datetime

try:
   client = MongoClient('localhost',27017)
   db = client["test"]
   col = db["pulse_rate"]
   print("Connected successfully to MongoDB")
except Exception as e:  
   print(f'Could not connect to MongoDB...\n error: {e}')

# Define server with port
bootstrap_servers = ['localhost:9092']

# Define topic name from where the message will recieve
topicName = 'test'
# recordCount = 0
# recordList = []
# Initialize consumer variable
consumer = KafkaConsumer(topicName, group_id ='group0', bootstrap_servers =
   bootstrap_servers)


print('Waiting for input from Producer...')
# Read and print message from consumer
for msg in consumer:
    print(f'Topic Name={msg.topic}, Pulse rate={msg.value}')
    message_json = msg.value.decode('utf-8')
    s = json.dumps(message_json, indent=4, sort_keys=True)
    record = json.loads(s)
    print(f'Document value: {record}')
    pulseRate = record

    try:
      pulse_rec = {'date': datetime.datetime.now(), 'pulseRate': pulseRate}
      pulse_rec_id = col.insert_one(pulse_rec)
    except Exception as e:
        print(f'Could not insert records into MongoDB...\n error: {e}')

# Terminate the script
sys.exit()