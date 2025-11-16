from kafka import KafkaConsumer
import psycopg2
import json

# Подключение к PostgreSQL
pg_conn = psycopg2.connect(
    dbname='test_db',
    user='admin',
    password='admin',
    host='localhost',
    port=5433
)
pg_cursor = pg_conn.cursor()

# Создаем Consumer ТОЛЬКО для PostgreSQL
consumer = KafkaConsumer(
    "user_events",
    bootstrap_servers='localhost:9092',
    group_id='postgres-consumer-group',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("🚀 PostgreSQL Consumer started - waiting for messages...")

try:
    for message in consumer:
        data = message.value
        print(f"📨 Received from Kafka: ID {data['id']}, User: {data['user']}")

        try:
            # Сохраняем в PostgreSQL (ИСПРАВЛЕННЫЙ запрос)
            pg_cursor.execute("""
                INSERT INTO user_logins (id, username, event_type, event_time) 
                VALUES (%s, %s, %s, to_timestamp(%s))
                ON CONFLICT (id) DO NOTHING
            """, (data['id'], data['user'], data['event'], data['timestamp']))
            pg_conn.commit()

            if pg_cursor.rowcount > 0:
                print(f"✅ Saved to PostgreSQL: ID {data['id']}")
            else:
                print(f"⚠️  Already exists in PostgreSQL: ID {data['id']}")

        except Exception as e:
            print(f"Error inserting into PostgreSQL: {e}")
            pg_conn.rollback()

except KeyboardInterrupt:
    print("PostgreSQL Consumer stopped")
    pg_cursor.close()
    pg_conn.close()
except Exception as e:
    print(f"Consumer error: {e}")