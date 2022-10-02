# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer
from pymongo import MongoClient
import json
# Import sys module
import os
import sys
from datetime import datetime
import requests
from dotenv import load_dotenv
import bson.json_util as json_util

load_dotenv()

try:
   client = MongoClient('localhost',27017)
   db = client["test"]
   col = db["pulse_rate"]
   print("Connected successfully to MongoDB")
   powerBI = os.getenv('POWERBI_API')
except Exception as e:  
   print(f'Could not connect to MongoDB...\n error: {e}')

# Define server with port
bootstrap_servers = ['localhost:9092']
# Define topic name from where the message will recieve
topicName = 'test'
# Initialize consumer variable
consumer = KafkaConsumer(topicName, group_id ='group0', bootstrap_servers =
   bootstrap_servers)


print('Waiting for input from Producer...')
# Read and print message from consumer
for msg in consumer:
    message_json = msg.value.decode('utf-8')
    s = json.dumps(message_json, indent=4, sort_keys=True)
    record = json.loads(s)
    pulseRate = record
    print(f'Pulse Rate: {pulseRate}')

    try:
      now = datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%S%Z")
      if pulseRate == '':
         pulseRate = '0.0'
      pulse_rec = {'date': now, 'pulseRate': float(pulseRate)}
      pulse_rec_id = col.insert_one(pulse_rec)
      try:
         pulse_stream = {'date': now, 'pulseRate': pulse_rec["pulseRate"]}
         response = requests.post(powerBI,data=json_util.dumps([pulse_stream]))
         print(f'API response code: {response.status_code}')
      except Exception as ex:
         print(f'Could not send data to PowerBI Dashboards...\n error: {ex}')
    except Exception as e:
        print(f'Could not insert records into MongoDB...\n error: {e}')

# Terminate the script
sys.exit()