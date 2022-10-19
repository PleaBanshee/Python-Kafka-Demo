# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import time
# Import sys module
import os
import sys
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
import bson.json_util as json_util

load_dotenv()

pulseRate = 0.0
threshold_val = ()
pulse_date = []
pulse_val = []
try:
   client = MongoClient('localhost',27017)
   db = client["test"]
   col = db["pulse_rates"]
   print("Connected successfully to MongoDB")
   powerBI = os.getenv('POWERBI_API')
except Exception as e:  
   print(f'Could not connect to MongoDB...\n error: {e}')

# Define server with port
bootstrap_servers = ['localhost:9092']
# Define topic name from where the message will recieve
topicName = 'kafka16'
# Initialize consumer variable
consumer = KafkaConsumer(topicName, group_id ='group0', bootstrap_servers =
   bootstrap_servers)

def calcLargestFactor(x):
   factors = []
   for i in range(1, x + 1):
       if x % i == 0:
           factors.append(i)
   return factors
    
def get_large(x):
    if (x % 2 != 0):
        return calcLargestFactor(x)[1]
    else:
        return calcLargestFactor(x)[-3]

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
    if avg >= 100:
        for item in threshold_check["Pulse_Val"]:
            if item >= 100 and counter == 0:
                threshold_start = counter
                threshold_list.append(threshold_check['Date'])
                counter += 1
                continue
            if item >= 100:
                threshold_list.append(threshold_check['Date'])
            counter += 1
        if threshold_check['Date'][threshold_start] == None:
            threshold_check['Date'][threshold_start] = now
        if threshold_check['Date'][len(threshold_list)-1] == None:
            threshold_check['Date'][len(threshold_list)-1] =  threshold_check['Date'][threshold_start] + timedelta(seconds=counter)
        return threshold_check['Date'][threshold_start],threshold_check['Date'][len(threshold_list)-1]

print('Waiting for input from Producer...')
# Read and print message from consumer
for msg in consumer:
   message_json = msg.value.decode('utf-8')
   s = json.dumps(message_json, indent=4, sort_keys=True)
   record = json.loads(s)
   pulseRate = record

   try:
      try:
         now = datetime.now().replace(tzinfo=None).isoformat() + 'Z'
         if pulseRate == '' or pulseRate == None:
            pulseRate = '0.0'
         pulse_rec = {'date': now, 'pulseRate': float(pulseRate)}
         pulse_rec_id = col.insert_one(pulse_rec)

         try:
            pulse_stream = {'date': now, 'pulseRate': pulse_rec["pulseRate"]}
            response = requests.post(powerBI,data=json_util.dumps([pulse_stream]))
            print(f'API response code: {response.status_code}')
            print(f'Pulse Rate: {pulseRate}')
            pulse_date.append(now)
            pulse_val.append(float(pulseRate))
            # time.sleep(1)
         except Exception as ex:
            print(f'Could not send data to PowerBI Dashboards...\n error: {ex}')
      except Exception as e:
         print(f'Could not insert records into MongoDB...\n error: {e}')
   except KeyboardInterrupt:
      # Terminate the script
      time1 = 0
      time2 = 0
      print('Calculating threshold timestamps...')
      time.sleep(5)
      largestFactor = get_large(len(pulse_val))
      chunkz = [pulse_date[i:i + len(pulse_date)//largestFactor] for i in range(0, len(pulse_date), len(pulse_date)//largestFactor)]
      chunkz2 = [pulse_val[i:i + len(pulse_val)//largestFactor] for i in range(0, len(pulse_val), len(pulse_val)//largestFactor)]
      for i in range(0,len(chunkz)):
         if time1 or time2 is None:
            if time1 == None:
                time1 = 0
            if time2 == None:
                time2 = 0
         time1 = calcThresholds(chunkz[i],chunkz2[i])[0]
         time2 = calcThresholds(chunkz[i],chunkz2[i])[1]
         print(f"Thresholds occured from {time1} to {time2}")
      print('Exiting Consumer...')
      if time1 == 0 and time2 == 0:
           print("No thresholds occured")
      client.close()
      sys.exit()