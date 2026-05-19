import clickhouse_connect

client = clickhouse_connect.get_client(
    host='clickhouse', 
    port=8123, 
    username='admin', 
    password='admin_pass'
)

# client.command('DROP TABLE IF EXISTS default.windowed_stats_summing ')
# client.command('DROP VIEW IF EXISTS default.windowed_stats_mv')

# в данном случае ORDER BY это ключ \/\/\/\/\/
client.command("""
    CREATE TABLE IF NOT EXISTS default.windowed_stats_summing
    (
        timer DateTime,
        category String,
        total_sum Decimal(18,2),
        tx_count Int64
    )
    ENGINE = SummingMergeTree()
    ORDER BY (timer, category)
""")
# в данном случае ORDER BY это ключ /\/\/\/\/\/\

client.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS default.windowed_stats_mv
    TO default.windowed_stats_summing
    AS
    SELECT
        toStartOfInterval(
            if(timestamp = 0, now(), toDateTime(timestamp)),
            INTERVAL 10 MINUTE
        ) AS timer,
        multiIf(
            upper(replaceRegexpAll(category, '\s+', '')) = '', 'UNKNOWN',
            upper(replaceRegexpAll(category, '\s+', '')) = 'N/A', 'UNKNOWN',
            upper(replaceRegexpAll(category, '\s+', '')) = 'NULL', 'UNKNOWN',
            upper(replaceRegexpAll(category, '\s+', ''))
        ) AS category,
        round (amount, 2) AS total_sum,
        1 AS tx_count
    FROM default.raw_transactions_kafka
""")
