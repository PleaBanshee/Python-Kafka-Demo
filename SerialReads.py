import serial

ser = serial.Serial('COM3',9800,timeout=1)

while True:
    packet = ser.readline()
    print(packet.decode('utf-8'))
    print(type(packet))
    print(type(packet.decode('utf-8')))
