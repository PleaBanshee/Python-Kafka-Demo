cd C:\Users\Llewellyn\Desktop\Python-Kafka-Demo\bats
START zookeeper.bat
START kafka.bat
timeout /t 10
START consumer.bat
START producer.bat
