# Import KafkaProducer from Kafka library
import serial
from kafka import KafkaProducer

ser = serial.Serial('COM3',9800,timeout=1)

# Define server with port
bootstrap_servers = ['localhost:9092']

# Define topic name where the message will publish
topicName = 'test'

# Initialize producer variable
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

print('Starting to read input from Arduino Uno R3...')
# Publish text in defined topic
while True:
    arduinoReads = ser.readline()
    producer.send(topicName,arduinoReads)
    print("Reading...")
    producer.flush()