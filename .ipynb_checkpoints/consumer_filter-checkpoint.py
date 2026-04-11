from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'lab1',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    if message.value['amount'] > 3000:
        print("Alert - wartość powyżej 3000")
    else:
        print(message.value)
    
# TWÓJ KOD
# Dla każdej wiadomości: sprawdź amount > 1000, jeśli tak — wypisz ALERT
