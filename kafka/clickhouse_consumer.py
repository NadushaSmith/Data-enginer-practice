from kafka import KafkaConsumer
from clickhouse_driver import Client
import psycopg2
import json
import os
from datetime import datetime

def clickhouse_client():
    return Client(
        host=os.getenv('CLICKHOUSE_HOST'),
        port=os.getenv('CLICKHOUSE_PORT'),
        user=os.getenv('CLICKHOUSE_USER'),
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        database=os.getenv('CLICKHOUSE_DB')
    )

def kafka_consumer():
    return KafkaConsumer(
        os.getenv('KAFKA_TOPIC', 'user_events'),
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        group_id='clickhouse_consumer_group',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def clickhouse_table_exist(client):
    client.execute("""
        CREATE TABLE IF NOT EXISTS user_logins (
            id Int32,
            username String,
            event_type String,
            event_time DateTime,
            processed_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (event_time, id)
    """)

def save_to_clickhouse(client, data):
    try:
        event_time = datetime.fromtimestamp(data['timestamp'])

        client.execute(
            "insert into user_logins (id, username, event_type, event_time) values"
            [(data['id'], data['user'], data['event'], event_time)]
        )
        return True
    
    except Exception as e:
            return False

def main():
    print("ClickHouse Consumer запущен")
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("Настройки загружены из .env")
    except ImportError:
        print("Библиотека python-dotenv не установлена, используем настройки по умолчанию")
    
    ch_client = None
    consumer = None

    try:
        ch_client = clickhouse_client()
        consumer = kafka_consumer()
        
        clickhouse_table_exist(ch_client)

        print("🔄 Ожидание сообщений из Kafka...")
        print("   Для остановки нажмите Ctrl+C\n")

        for message in consumer:
            data = message.value
            print(f"📨 Получено из Kafka: ID {data['id']}, User: {data['user']}")

            success = save_to_clickhouse(ch_client, data)

            if success:
                processed_count += 1
                print(f"✅ Сохранено в ClickHouse: ID {data['id']}")
            else:
                error_count += 1
    except KeyboardInterrupt:
        print(f"\n🛑 ClickHouse Consumer остановлен")
        print(f"📊 Итоги: Обработано: {processed_count}, Ошибок: {error_count}")
    
    except Exception as e:
        print(f"❌ Критическая ошибка консьюмера: {e}")
    
    finally:
        # Корректно закрываем соединения
        try:
            if ch_client:
                ch_client.disconnect()
            if consumer:
                consumer.close()
            print("🔌 Соединения закрыты")
        except Exception as e:
            print(f"⚠️  Ошибка при закрытии соединений: {e}")

if __name__ == "__main__":
    main()






     
                
         

