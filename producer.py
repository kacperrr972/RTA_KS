from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092', 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sklepy = ['Warszawa', 'Kraków', 'Gdańsk', 'Wrocław']
kategorie = ['elektronika', 'odzież', 'żywność', 'książki']

def score_transaction(tx):
    score = 0
    rules = []
    
    if tx['amount'] > 3000:
        score += 3
        rules.append('R1')
        
    if tx['category'] == 'elektronika' and tx['amount'] > 1500:
        score += 2
        rules.append('R2')
        
    if tx['hour'] < 6:
        score += 2
        rules.append('R3')
        
    return score, rules


def generate_transaction():
    if random.random() < 0.05:
        tx = {
            'tx_id': f'TX{random.randint(1000,9999)}',
            'user_id': f'u{random.randint(1,20):02d}',
            'amount': round(random.uniform(3000.01, 5000.0), 2), 
            'store': random.choice(sklepy),
            'category': 'elektronika',                           
            'timestamp': datetime.now().isoformat(),
            'hour': random.randint(0, 5)                         
        }
    else:
        tx = {
            'tx_id': f'TX{random.randint(1000,9999)}',
            'user_id': f'u{random.randint(1,20):02d}',
            'amount': round(random.uniform(5.0, 5000.0), 2),
            'store': random.choice(sklepy),
            'category': random.choice(kategorie),
            'timestamp': datetime.now().isoformat(),
            'hour': random.randint(6, 23)                        
        }
        
    score, rules = score_transaction(tx)
    
    tx['score'] = score
    tx['triggered_rules'] = rules
    tx['is_suspicious'] = score >= 3 
    
    return tx


for i in range(1000):
    tx = generate_transaction()
    producer.send('lab1', value=tx)
    
    alert = "[! PODEJRZANA !]" if tx['is_suspicious'] else ""
    print(f"[{i+1}] {tx['tx_id']} | {tx['amount']:.2f} PLN | Score: {tx['score']} {alert}")
    
    time.sleep(0.5)

producer.flush()
producer.close()
