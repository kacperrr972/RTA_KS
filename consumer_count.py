from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json
import time

consumer = KafkaConsumer(
    'lab1',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0


for message in consumer:
    tx = message.value
    
    store_counts[tx['store']] += 1
    total_amount[tx['store']] += tx['amount']
    
    msg_count += 1
    
    print(f"[{msg_count}] {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']}")
    
    if msg_count % 10 == 0:
        print("\n--- PODSUMOWANIE PO", msg_count, "WYSŁAŃ ---")
        print("Liczba transakcji w sklepach:", dict(store_counts))
        print("------------------------------------\n")
        
    time.sleep(0.5)
