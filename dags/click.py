import clickhouse_connect
from datetime import datetime

client = clickhouse_connect.get_client(
    host='clickhouse', 
    port=8123, 
    username='admin', 
    password='admin_pass'
)

client.command("""
    CREATE TABLE IF NOT EXISTS test_table (
        id Int64,
        user String,
        ts DateTime
    ) 
    ENGINE = MergeTree() 
    ORDER BY (ts, id)
""")

print("table is created")

print("генерация")
rows = [
    [i, f'user_{i}', datetime.datetime.now()] 
    for i in range(100000)
]

client.insert('test_table', rows, column_names=['id', 'user', 'ts'])
print("100к строк улетели")

print("куски данных:")
parts_info = client.query("""
    SELECT name, rows, bytes_on_disk 
    FROM system.parts 
    WHERE table = 'test_table' AND active = 1
""")
for part in parts_info.result_set:
    print(f"кусок: {part[0]} | Строк: {part[1]} | размер: {part[2]} байт")

print("данные в клике")

result = client.query('SELECT * FROM test_table LIMIT 1000') # лимит 1к чтобы консоль не померла, а то я мандавоз 100к захотел принтить
for row in result.result_set:
    print(row)
