import psycopg2
from kafka import KafkaProducer
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

def get_postgres_connect():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
      
    )

def kafka_producer():
    return KafkaProducer(
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def send_unsert_records(producer, cursor, conn):
    cursor.execute("""
        select id, username, event_type, event_time
        from user_logins
        where sent_to_kafka = FALSE
        order by id
    """)
    unsert_records = cursor.fetchall()

    if not unsert_records:
        print("не отправленные записи не найдены")
        return 0
    print(f"найдено {len(unsert_records)} неотправленных записей. Отправляем в Кафка")

    sent_count = 0
    errors_count = 0

    for record in unsert_records:
        id, username, event_type, event_time = record

        message_data = {
            'id': id,
            'user': username,
            'event': event_type,
            'timestamp': event_time.timestamp()
        }

        try:
            producer.send(
                os.getenv('KAFKA_TOPIC', 'user_events'),
                value=message_data
            )
            
            cursor.execute(
                'update user_logins set sent_to_kafka = true where id = %s', (id)
            )
            conn.commit()

            sent_count += 1
            print(f"[{sent_count}/{len(unsert_records)}] Отправлено: ID (id), User: {username}")
        except Exception as e:
            errors_count += 1
            print(f"Ошибка отправки ID {id}: {e}")
            conn.rolback()
    return sent_count, errors_count

def main():
    print("Kafka запущен!")
    try:
        pg_conn = get_postgres_connect()
        pg_cursor = pg_conn.cursor()
        producer = kafka_producer()

        unsert_count = send_unsert_records(pg_cursor)

        if unsert_count == 0:
            print("""
💡 Совет: 
- Запустите init_database.py для создания тестовых данных
- Или добавьте данные вручную в таблицу user_logins
            """)
        else:
            sent_count, errors_count = send_unsent_records(producer, pg_cursor, pg_conn)

            producer.flush()

            print(f" Успешно отпраавлено: {sent_count}")
            print(f" Ошибок: {errors_count}")
            print(f" Всего обработано: {sent_count + errors_count}")

            if errors_count > 0:
                print(f" ⚠️  Некоторые записи не отправлены. Запустите повторно.")
    except Exception as e:
        print(f"Критические ошибки: {e}")
    finally:
        try:
            pg_cursor.close()
            pg_conn.close()
            producer.close()
            print(" Соединение закрыто")
        except:
            pass
if __name__ == "__main__":
    main()


  