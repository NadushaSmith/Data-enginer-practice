from kafka import KafkaConsumer
import psycopg2
from clickhouse_driver import Client
import json
from datetime import datetime


# Подключение к Clickhouse
ch_client = Client(
    host='localhost',
    port=9001,
    user='user',
    password='strongpassword',
    database='kafka_course'
)
# создаем Consumer
consumer = KafkaConsumer(
    "user_events",
    bootstrap_servers='localhost:9092',
    group_id='clickhouse_consumer_group',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
print("ClickHouse Consumer started - waiting for message...")

try:
    for message in consumer:
        data = message.value
        print("Received from Kafka: ID {data['id']}, User: {data['user']}")

        try:
            # Сохраняем в Clickhouse
            event_time = datetime.fromtimestamp(data['timestamp'])
            ch_client.execute(
                "INSERT INTO user_logins (id, username, event_type, event_time) values",
                [(data['id'], data['user'], data['event'], event_time)]
            )
            print(f"Save to ClickHouse: ID {data['id']}")

        except Exception as e:
            print(f"Error inserting into Clickhouse: {e})")
except KeyboardInterrupt:
    print("ClickHouse Consumer stopped")
except Exception as e:
    print(f"Consumer error: {e}")





