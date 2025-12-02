# from pymongo import MongoClient
# from datetime import datetime
#
# # подключение к базе данных Mongodb
# client = MongoClient("mongodb://localhost:27017/")  # ← добавить / в конце
# db = client['my_database']
#
# # ОЧИСТКА КОЛЛЕКЦИЙ
# #db.user_events.delete_many({})
# #db.archive_users.delete_many({})
# print("🗑️ Коллекции очищены")
#
# collection = db["user_events"]
#
# data = [
#     {
#         "user_id": 123,
#         "event_type": "purchase",
#         "event_time": datetime(2024, 1, 20, 10, 0, 0),
#         "user_info": {
#             "email": "user1@example.com",
#             "registration_date": datetime(2023, 12, 1, 10, 0, 0)
#         }
#     },
#     {
#         "user_id": 124,
#         "event_type": "login",
#         "event_time": datetime(2024, 1, 21, 9, 30, 0),
#         "user_info": {
#             "email": "user2@example.com",
#             "registration_date": datetime(2023, 12, 2, 12, 0, 0)
#         }
#     },
#     {
#         "user_id": 125,
#         "event_type": "signup",
#         "event_time": datetime(2024, 1, 19, 14, 15, 0),
#         "user_info": {
#             "email": "user3@example.com",
#             "registration_date": datetime(2023, 12, 3, 11, 45, 0)
#         }
#     },
#     {
#         "user_id": 126,
#         "event_type": "purchase",
#         "event_time": datetime(2024, 1, 20, 16, 0, 0),
#         "user_info": {
#             "email": "user4@example.com",
#             "registration_date": datetime(2023, 12, 4, 9, 0, 0)
#         }
#     },
#     {
#         "user_id": 127,
#         "event_type": "login",
#         "event_time": datetime(2024, 1, 22, 10, 0, 0),
#         "user_info": {
#             "email": "user5@example.com",
#             "registration_date": datetime(2023, 12, 5, 10, 0, 0)
#         }
#     },
#     {
#         "user_id": 128,
#         "event_type": "signup",
#         "event_time": datetime(2024, 1, 22, 11, 30, 0),
#         "user_info": {
#             "email": "user6@example.com",
#             "registration_date": datetime(2023, 12, 6, 13, 0, 0)
#         }
#     },
#     {
#         "user_id": 129,
#         "event_type": "purchase",
#         "event_time": datetime(2024, 1, 23, 15, 0, 0),
#         "user_info": {
#             "email": "user7@example.com",
#             "registration_date": datetime(2023, 12, 7, 8, 0, 0)
#         }
#     },
#     {
#         "user_id": 130,
#         "event_type": "login",
#         "event_time": datetime(2024, 1, 23, 16, 45, 0),
#         "user_info": {
#             "email": "user8@example.com",
#             "registration_date": datetime(2023, 12, 8, 10, 0, 0)
#         }
#     },
#     {
#         "user_id": 131,
#         "event_type": "purchase",
#         "event_time": datetime(2024, 1, 24, 12, 0, 0),
#         "user_info": {
#             "email": "user9@example.com",
#             "registration_date": datetime(2023, 12, 9, 14, 0, 0)
#         }
#     },
#     {
#         "user_id": 132,
#         "event_type": "signup",
#         "event_time": datetime(2024, 1, 24, 18, 30, 0),
#         "user_info": {
#             "email": "user10@example.com",
#             "registration_date": datetime(2023, 12, 10, 10, 0, 0)
#         }
#     }
# ]
#
# # Заливка данных в коллекцию
# collection.insert_many(data)
# print("✅ Данные успешно загружены в MongoDB")
#
# client.close()


from pymongo import MongoClient
from datetime import datetime, timedelta
import json
import os

# подключение к базе данных Mongodb
client = MongoClient("mongodb://localhost:27017/")
db = client['my_database']
user_events = db["user_events"]  # основная коллекция
archive_users = db["archive_users"]  # архивная коллекция

# Устанавливаем дату для расчета
today = datetime(2024, 2, 8)

# Рассчитываем временные границы
users_registered = today - timedelta(days=30)  # ← исправить опечатку
users_noactive = today - timedelta(days=14)

print("🔍 Критерии поиска:")
print(f"📅 Сегодня: {today.strftime('%Y-%m-%d')}")
print(f"👴 Зарегистрированы до: {users_registered.strftime('%Y-%m-%d')}")
print(f"💤 Неактивны с: {users_noactive.strftime('%Y-%m-%d')}")

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
print(f"📊 Количество пользователей для архивации: {len(users_to_archive)}")

if users_to_archive:
    # Архивируем (только копируем, не удаляем)
    archive_users.insert_many(users_to_archive)
    user_ids = [user["_id"] for user in users_to_archive]  # ← исправить на user_ids
    print(f"✅ Внесено в архив: {len(users_to_archive)} пользователей")

    # Покажем кого заархивировали
    print("👥 Заархивированные пользователи:")
    for user in users_to_archive:
        inactive_days = (today - user["last_activity"]).days
        print(f"   - User {user['_id']}: неактивен {inactive_days} дней")

result_report = {
    "date": today.strftime("%Y-%m-%d"),
    "archived_users_count": len(users_to_archive),
    "archived_user_ids": [user["_id"] for user in users_to_archive]  # ← исправить опечатку
}

os.makedirs("reports", exist_ok=True)
report_file = os.path.join("reports", f"{today.strftime('%Y-%m-%d')}.json")

try:
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(result_report, f, indent=2, ensure_ascii=False)
    print(f"📄 Отчет сохранен: {report_file}")
except Exception as e:
    print(f"❌ Ошибка сохранения отчета: {e}")

client.close()