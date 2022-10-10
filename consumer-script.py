# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import time
import os
# Import sys module
import sys
from datetime import datetime
import requests
from dotenv import load_dotenv
import bson.json_util as json_util

load_dotenv()

try:
   pulseRate = 0.0
   threshold_val = ()
   pulse_date = []
   pulse_val = []
   global_record = {}
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
topicName = 'test-script1'
# Initialize consumer variable
consumer = KafkaConsumer(topicName, group_id ='group0', bootstrap_servers =
   bootstrap_servers,auto_offset_reset='earliest')

def calcThresholds(date,pulse):
    sum_total = 0
    counter = 0
    threshold_start = 0
    threshold_check = {
        "Date": date,
        "Pulse_Val": pulse
    }
    sumThreshold = pulse
    for num in sumThreshold:
        sum_total += num
    avg = sum_total//len(pulse)
    threshold_list = []
    if avg > 200:
        for item in threshold_check["Pulse_Val"]:
            if item > 200:
                threshold_start = counter
                break
            if item > 200:
                threshold_list.append(threshold_check['Date'])
            counter += 1
      #   print(f"Threshold Exceeded from time {threshold_check['Date'][threshold_start]} to {threshold_check['Date'][9]}")
        return threshold_check['Date'][threshold_start],threshold_check['Date'][len(threshold_list)-1]



print('Waiting for input from Producer...')
# Read and print message from consumer
for msg in consumer:
   message_json = msg.value.decode()
   s = json.dumps(message_json, indent=4, sort_keys=True)
   record = json.loads(s)
   pulseRate = record
   print(f'Pulse Rate: {pulseRate}')

   try:
      try:
         now = datetime.strftime(datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
         if pulseRate == '':
            pulseRate = '0.0'
         pulse_rec = {'date': now, 'pulseRate': float(pulseRate)}
         pulse_rec_id = col.insert_one(pulse_rec)
         pulse_date.append(now)
         pulse_val.append(float(pulseRate))
         try:
            pulse_stream = {'date': now, 'pulseRate': pulse_rec["pulseRate"]}
            response = requests.post(powerBI,data=json_util.dumps([pulse_stream]))
            print(f'API response code: {response.status_code}')
            # time.sleep(1)
         except Exception as ex:
            print(f'Could not send data to PowerBI Dashboards...\n error: {ex}')
      except Exception as e:
         print(f'Could not insert records into MongoDB...\n error: {e}')
   except KeyboardInterrupt:
      # Terminate the script
      threshold_val = calcThresholds(pulse_date, pulse_val)
      print(f"Threshold Exceeded from time: {threshold_val}")
      print('Exiting Consumer...')
      sys.exit()