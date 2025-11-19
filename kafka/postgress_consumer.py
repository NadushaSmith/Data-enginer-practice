from kafka import KafkaConsumer
import psycopg2
import json
import os
from datetime import datetime

# Подключение к PostgreSQL
def get_postgres_connect():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        dbname=os.getenv('POSTGRES_DB'),
        user=os.gete('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

def kafka_consumer():
    return KafkaConsumer(
        os.getenv('KAFKA_TOPIC', 'user_events'),
        bootstrap_servers='KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        group_id='postgres-consumer-group',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    
def ensure_consumer_table_exists(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_logins_consumer (
            id INTEGER PRIMARY KEY,
            username VARCHAR(100) NOT NULL,
            event_type VARCHAR(50) NOT NULL,
            event_time TIMESTAMP NOT NULL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            kafka_offset BIGINT,
            kafka_partition INTEGER
        )

    """)

def save_to_postgres(cursor, date, message):
    try:
        event_time = datetime.fromtimestamp(data['timestamp'])

        cursor.execute("""
            insert into user_logins_consumer
            (id, username, event_type, event_time, kafka_offset, kafka_partition)
            values (%s, %s, %s, %s, %s, %s)
            on conflict (id) do nothing
            """, (
                data['id'],
                data['user'],
                data['event'],
                event_time,
                message.offset,
                message.partition
        ))
        conn.commit()

        if cursor.rowcount():
            print(f"Сохранено в Postgress: ID {data['id']}, User: {data['user']}")
            return True
        else:
            print(f"⚠️  Уже существует в PostgreSQL: ID {data['id']}")
            return False
    except Exception as e:
        print(f"❌ Ошибка сохранения в PostgreSQL ID {data['id']}: {e}")
        conn.rollback()
        return False

def main():
    print("Consumer PostgreSQL запущен")

    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("✅ Настройки загружены из .env")
    except ImportError:
         print("ℹ️  Библиотека python-dotenv не установлена, используем настройки по умолчанию")
    pg_conn = None
    pg_cursor = None
    consumer = None

    try:
        pg_conn = get_postgres_connect()
        pg_cursor = pg_conn.cursor()
        consumer = kafka_consumer()

        ensure_consumer_table_exists(pg_cursor)
        pg_conn.commit()

        procecced_count = 0
        error_count = 0

        for message in consumer:
            data = message.value
            print(f"📨 Получено из Kafka: ID {data['id']}, User: {data['user']}")
            success = save_to_postgres(pg_cursor, pg_conn, data, message)

            if success:
                procecced_count += 1
            else:
                error_count += 1
    except KeyboardInterrupt:
        print(f"\n PostgreSQL Consumer остановлен")
        print(f"Обработано: {procecced_count}, Ошибок: {error_count}")
    except Exception as e:
         print(f"Критическая ошибка консьюмера: {e}")

    finally:
        try:
            if pg_cursor:
                pg_cursor.close()
            if pg_conn:
                pg_conn.close()
            if consumer:
                consumer.close()
            print("Соединения закрыты")

        except Exception as e:
            print(f"Ошибка при закрытии соединений: {e}")

if __name__ == "__main__":
    main()




        

            



    




# Создаем Consumer ТОЛЬКО для PostgreSQL
consumer = KafkaConsumer(
    "user_events",
    

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