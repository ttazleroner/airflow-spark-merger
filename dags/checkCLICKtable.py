import clickhouse_connect

client = clickhouse_connect.get_client(
    host='clickhouse', 
    port=8123, 
    username='admin', 
    password='admin_pass'
)

rezultat = client.query("""
    SELECT * FROM default.raw_transactions_mergetree FINAL
    ORDER BY event_time DESC
    LIMIT 100
""")

if not rezultat.result_set:
    print("пусто")
else:
    for row in rezultat.result_set:
        print(row)