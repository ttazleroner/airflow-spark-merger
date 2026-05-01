import csv
import json
import random
from kafka import KafkaProducer
cat = ['electro', 'food', 'auto', 'beauty']
types = ['transfer', 'refund', 'purchase']
segment = ['standart', 'premium', 'vip']

producer = KafkaProducer(
    bootstrap_servers=['kafka_broker:29092'],
    api_version=(3, 7, 0),
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

csv_file = '/home/jovyan/work/data/raw/dirty_transactions_1gb.csv'

def clean(val):
    try:
        return float(str(val).replace(',', '.').strip())
    except:
        return 0.0
    
with open(csv_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        row['class'] = random.choice(segment)
        row['amount'] = clean(row.get('amount', 0))
        if not row.get('category'):
            row['category'] = random.choice(cat)
        row['type'] = random.choice(types)
        producer.send('raw_transactions', key=row.get('user'),value=row)
        if i % 2000 == 0:
            print(f"обработано {i} строк")
            producer.flush()
        if i > 20000: 
            break 

producer.flush()
