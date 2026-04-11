from kafka import KafkaConsumer, KafkaProducer
import json
import time

consumer = KafkaConsumer('lab1', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for message in consumer:
    tx = message.value
    if tx.get('score', 0) >= 3:
        print(f"Podejrzana transakcja: {tx['tx_id']} | Kwota: {tx['amount']} PLN | Reguły: {tx['triggered_rules']}")
        alert_producer.send('alerts', value=tx)
        time.sleep(0.5)
        
