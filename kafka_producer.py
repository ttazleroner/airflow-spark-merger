import csv
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['kafka_broker:29092'],
    api_version=(3, 7, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

csv_file = '/home/jovyan/work/data/raw/dirty_transactions_1gb.csv'

with open(csv_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        producer.send('raw_transactions', value=row)
        if i % 1000 == 0: producer.flush()
        if i > 10000: break 

producer.flush()