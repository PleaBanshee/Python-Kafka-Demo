# Import KafkaProducer from Kafka library
import sys
import serial
import time
from kafka import KafkaProducer

ser = serial.Serial('COM3',9800)

# Define server with port
bootstrap_servers = ['localhost:9092']

# Define topic name where the message will publish
topicName = 'kafka16'

# Initialize producer variable
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

print('Starting to read input from Arduino Uno R3...')
# Publish text in defined topic
while True:
    try:
        arduinoReads = ser.readline()
        producer.send(topicName,arduinoReads)
        print("Reading...")
        producer.flush()
        # time.sleep(1)
    except KeyboardInterrupt:
        # Terminate the script
        print('Exiting Producer...')
        sys.exit()