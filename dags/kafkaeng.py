import clickhouse_connect

client = clickhouse_connect.get_client(
    host='clickhouse', 
    port=8123, 
    username='admin', 
    password='admin_pass'
)


# client.command('DROP VIEW IF EXISTS default.raw_transactions_mv ')
# client.command('DROP TABLE IF EXISTS default.raw_transactions_mergetree')
# client.command('DROP TABLE IF EXISTS default.raw_transactions_kafka')

client.command("""
    CREATE TABLE IF NOT EXISTS default.raw_transactions_mergetree
    (
        id Int64,
        user String,
        amount Float64,
        timestamp Int64,
        category String,
        event_time DateTime MATERIALIZED toDateTime(timestamp)
    )
    ENGINE = MergeTree()
    ORDER BY (category, event_time, id)
    PARTITION BY toYYYYMM(event_time)
""")

client.command("""
    CREATE TABLE IF NOT EXISTS default.raw_transactions_kafka
    (
        id Int64,
        user String,
        amount Float64,
        timestamp Int64,
        category String
    )
    ENGINE = Kafka()
    SETTINGS
        kafka_broker_list = 'kafka_broker:29092',
        kafka_topic_list = 'raw_transactions',
        kafka_group_name = 'clickhouse_raw_group',
        kafka_format = 'JSONEachRow',
        kafka_skip_broken_messages = 100
""")

client.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS default.raw_transactions_mv
    TO default.raw_transactions_mergetree
    AS
    SELECT
        id,
        user,
        category,
        timestamp,
        amount
    FROM default.raw_transactions_kafka
""")































































# import clickhouse_connect
# import os


# click_client = clickhouse_connect.get_client(
#     host='clickhouse', 
#     port=8123, 
#     username='admin', 
#     password='admin_pass'
# )

# client.command("""
#     CREATE TANLE IF NOT EXISTS def.raw.trans_mergethree
#     (
#         id Int64,
#         user String,
#         amount Int64,
#         timestamp Int64,
#         category String,
#         event_time DateTime MATERIALIZED toDateTime(timestamp)
#     )
#     ENGINE = MergeTree()
#     ORDER BY (category,event_time,id)
#     PARTITION BY toYYYYMM(event_time)
# """)

# client.command("""
#     CREATE TABLE IF NOT EXISTS def.raw.trans_kafka
#     (
#         id Int64,
#         user String,
#         amount Int64,
#         timestamp Int64,
#         category String
#     )
#     ENGINE = Kafka()
#     SETTINGS
#         kafka_broker = 'kafka_broker:29092',
#         kafka_topic = 'raw_transactions',
#         group_name = 'clickhouse_raw_group',
#         kafka_format = 'JSONEachRow',
#         kafka_skip_broken_messages = 100
# """)


# client.command("""
#     CREATE MATERIALIZED VIEW IF NOT EXISTS def.raw.trans_mv
#     TO def.raw.trans_mergethree
#     AS
#     SELECT
#         id,
#         user,
#         category,
#         timestamp,
#         amount
#     FROM def.raw.trans_kafka
# """)
