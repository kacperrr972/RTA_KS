from kafka import KafkaConsumer
import json
import time


consumer = KafkaConsumer(
    'lab1',
    bootstrap_servers='broker:9092' ,
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
    

for message in consumer:
    tx = message.value
    amount = tx.get('amount', 0)
    
    if amount > 3000:
        tx['risk_level'] = "HIGH"
    elif amount > 1000:
        tx['risk_level'] = "MEDIUM"
    else:
        tx['risk_level'] = "LOW"
            
    print(f"TX: {tx['tx_id']} | Kwota: {amount:>7.2f} PLN | Ryzyko: {tx['risk_level']}")
        
    time.sleep(0.5)

