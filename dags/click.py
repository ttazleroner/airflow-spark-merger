import clickhouse_connect
from datetime import datetime

# 1. Подключаемся к Клику
# host='clickhouse' — это имя твоего контейнера в docker-compose
client = clickhouse_connect.get_client(
    host='clickhouse', 
    port=8123, 
    username='admin', 
    password='admin_pass'
)

# 2. Создаем таблицу (MergeTree)
client.command("""
    CREATE TABLE IF NOT EXISTS test_table (
        id Int64,
        user String,
        ts DateTime
    ) 
    ENGINE = MergeTree() 
    ORDER BY (ts, id)
""")

print("Таблица создана!")

# 1. Генерим 100 000 строк в памяти (это быстро)
print("Генерирую данные...")
rows = [
    [i, f'user_{i}', datetime.datetime.now()] 
    for i in range(100000)
]

# 2. Заливаем ОДНИМ батчем
# Клик обожает такие пачки
client.insert('test_table', rows, column_names=['id', 'user', 'ts'])
print("100,000 строк улетели!")

# 3. Самое интересное: смотрим на физические куски (parts) на диске
print("Физические куски данных в MergeTree:")
parts_info = client.query("""
    SELECT name, rows, bytes_on_disk 
    FROM system.parts 
    WHERE table = 'test_table' AND active = 1
""")
for part in parts_info.result_set:
    print(f"Кусок: {part[0]} | Строк: {part[1]} | Размер: {part[2]} байт")

print("Данные улетели в Клик!")

result = client.query('SELECT * FROM test_table')
for row in result.result_set:
    print(row)
