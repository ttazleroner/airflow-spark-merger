import json
from kafka import KafkaConsumer

topic = 'raw_transactions'
group_id = 'debug_group'

def json_des(data):
    try:
        return json.loads(data.decode('utf-8'))
    except Exception as e:
        print(f'ошибка парсинга {e}')
        return None

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=['kafka_broker:29092'],
    api_version=(3, 7, 0),
    value_deserializer=json_des,
    group_id=group_id,
    enable_auto_commit=True,
    auto_offset_reset='earliest'
)
try:
    for message in consumer:
        data = message.value
        if not data: continue
        try:
            raw_amount=str(data.get('amount',0)).replace(',', '.')
            amount=float(raw_amount)
            if amount > 15000:
                print(f'вери биг транзакция: {amount} | юзер: {data.get("user")}')
        except ValueError as e:
            print(f'кривые данные {e}')

except KeyboardInterrupt:
    print("стоп")
finally:
    consumer.close()