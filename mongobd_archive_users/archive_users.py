from pymongo import MongoClient
from datetime import datetime, timedelta
import json
import os

# подключение к базе данных Mongodb
client = MongoClient("mongodb://localhost:27017/")
db = client['my_database']
user_events = db["user_events"]  # основная коллекция
archive_users = db["archive_users"]  # архивная коллекция


# Расчитаем временные границы
today = datetime(2024, 2, 8)
users_registered = (today - timedelta(days=30))
users_noactive = (today - timedelta(days=14))

print("Критерии поиска:")
print(f"Сегодня: {today.strftime('%Y-%m-%d')}")
print(f"Зарегистрированы до: {users_registered.strftime('%Y-%m-%d')}")
print(f"Неактивны с: {users_noactive.strftime('%Y-%m-%d')}")

# Сгруппируем данные по условиям и фильтрам
pipeline = [
    {
        "$group": {
            "_id": "$user_id",
            "last_activity": {"$max": "$event_time"},
            "registration_date": {"$first": "$user_info.registration_date"}
        }
    },
    {
        "$match": {
            "registration_date": {"$lt": users_registered},  # регистрация до 30 дней
            "last_activity": {"$lt": users_noactive}  # активность до 14 дней
        }
    }
]

# поиск пользователей для архивации
users_to_archive = list(user_events.aggregate(pipeline))
print(f"Количество пользователй для архивации: {len(users_to_archive)}")

if users_to_archive:
    user_id = [user["_id"] for user in users_to_archive]
    print(f"Внесено в архив: {len(users_to_archive)} пользователей")
#
result_report = {
    "date": today.strftime("%Y-%m-%d"),
    "archived_users_count": len(users_to_archive),
    "archive_users_id": [user["_id"] for user in users_to_archive]
}

os.makedirs("reports", exist_ok=True)
report_file = os.path.join("reports", f"{today.strftime('%Y-%m-%d')}.json")

try:
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(result_report, f, indent=2, ensure_ascii=False)
        print(f"Отчет сохранен: {report_file}")
except Exception as e:
    print(f"Ошибка сохранения"
          f" отчета: {e}")