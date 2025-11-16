import psycopg2
from kafka import KafkaProducer
import json
import time

pg_conn = psycopg2.connect(
    dbname='test_db', user='admin', password='admin', host='localhost', port=5433
)
pg_cursor = pg_conn.cursor()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

test_records = [
    {'user': 'alice', 'event': 'login'},
    {'user': 'bob', 'event': 'purchase'},
    {'user': 'carol', 'event': 'view'},
    {'user': 'dave', 'event': 'logout'},
    {'user': 'eve', 'event': 'register'},
    {'user': 'frank', 'event': 'login'},
    {'user': 'grace', 'event': 'purchase'},
    {'user': 'henry', 'event': 'view'},
    {'user': 'ivy', 'event': 'logout'},
    {'user': 'jack', 'event': 'register'},
    {'user': 'karen', 'event': 'login'},
    {'user': 'leo', 'event': 'purchase'},
    {'user': 'mia', 'event': 'view'},
    {'user': 'nathan', 'event': 'logout'},
    {'user': 'olivia', 'event': 'register'}
]


def check_and_create_data():
    """Проверяет есть ли данные и создает если нет"""

    # Проверяем есть ли записи в PostgreSQL
    pg_cursor.execute("SELECT COUNT(*) FROM user_logins")
    count = pg_cursor.fetchone()[0]

    if count == 0:
        print("📝 В PostgreSQL нет данных. Создаем тестовые записи...")
        create_test_data()
        return True
    else:
        print(f"📊 В PostgreSQL есть {count} записей")
        return False


def create_test_data():
    """Создает тестовые данные"""

    for i, record in enumerate(test_records, 1):
        data = {
            'user': record['user'],
            'event': record['event'],
            'timestamp': time.time() + i
        }

        # Вставляем в PostgreSQL с sent_to_kafka = FALSE
        pg_cursor.execute(
            "INSERT INTO user_logins (username, event_type, event_time, sent_to_kafka) VALUES (%s, %s, to_timestamp(%s), %s) RETURNING id",
            (data['user'], data['event'], data['timestamp'], False)
        )

        record_id = pg_cursor.fetchone()[0]
        pg_conn.commit()

        print(f"✅ [{i}/{len(test_records)}] Создана запись: ID {record_id}, User: {data['user']}")


def send_unsent_records():
    """Отправляет непосланные записи в Kafka"""

    pg_cursor.execute("""
        SELECT id, username, event_type, event_time
        FROM user_logins 
        WHERE sent_to_kafka = FALSE
    """)

    unsent_records = pg_cursor.fetchall()

    if not unsent_records:
        print("Нет непосланных записей!")
        return 0

    print(f"Найдено {len(unsent_records)} непосланных записей. Отправляем в Kafka...")

    sent_count = 0
    for record in unsent_records:
        id, username, event_type, event_time = record

        message_data = {
            'id': id,
            'user': username,
            'event': event_type,
            'timestamp': event_time.timestamp()
        }

        try:
            producer.send('user_events', value=message_data)

            # Обновляем флаг
            pg_cursor.execute(
                "UPDATE user_logins SET sent_to_kafka = TRUE WHERE id = %s",
                (id,)
            )
            pg_conn.commit()

            sent_count += 1
            print(f"🚀 [{sent_count}/{len(unsent_records)}] Отправлено в Kafka: ID {id}")

        except Exception as e:
            print(f"Ошибка: {e}")
            pg_conn.rollback()

    return sent_count


if __name__ == "__main__":
    print("Working Producer запущен")

    # 1. Проверяем и создаем данные если нужно
    created_new = check_and_create_data()

    # 2. Отправляем непосланные записи
    sent_count = send_unsent_records()

    print(f"\n Итог: Отправлено записей: {sent_count}")

    producer.flush()
    pg_cursor.close()
    pg_conn.close()