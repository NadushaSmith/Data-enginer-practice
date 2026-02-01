"""
АВТОГЕНЕРИРУЕМЫЙ MASTER DAG
Создан: 2026-02-01 16:45:19
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="master_generated_dag",
    description="Автогенерируемый DAG из всех скриптов",
    start_date=datetime(2024, 1, 26),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["auto-generated", "master"],
) as dag:

    
    
    # Скрипт: check_data.py
# METADATA:
    # schedule: @daily
    # start_date: today
    # description: Ежедневная проверка качества данных
    # tasks: check_null_values, check_duplicates, validate_data_types

    import datetime

    def check_null_values():
        """Проверка на NULL значения в ключевых полях"""
        print("🔍 Проверка NULL значений...")
        # В реальности здесь был бы SQL запрос
        print("   SELECT COUNT(*) FROM users WHERE email IS NULL")
        result = {"table": "users", "null_count": 0, "status": "PASSED"}
        print(f"✅ Проверка завершена: {result}")
        return result

    def check_duplicates():
        """Проверка на дубликаты"""
        print("🔍 Проверка дубликатов...")
        print("   SELECT email, COUNT(*) FROM users GROUP BY email HAVING COUNT(*) > 1")
        result = {"table": "users", "duplicate_count": 0, "status": "PASSED"}
        print(f"✅ Проверка завершена: {result}")
        return result

    def validate_data_types():
        """Валидация типов данных"""
        print("🔍 Валидация типов данных...")
        print("   SELECT column_name, data_type FROM information_schema.columns")
        result = {"checked_tables": 3, "invalid_types": 0, "status": "PASSED"}
        print(f"✅ Проверка завершена: {result}")
        return result

    # Вывод для отладки (не войдёт в задачи)
    print("Скрипт проверки качества данных загружен")
    print(f"Время: {datetime.datetime.now()}")

    
    check_data_check_null_values = PythonOperator(
        task_id="check_data_check_null_values",
        python_callable=check_null_values,
    )
    
    check_data_check_duplicates = PythonOperator(
        task_id="check_data_check_duplicates",
        python_callable=check_duplicates,
    )
    
    check_data_validate_data_types = PythonOperator(
        task_id="check_data_validate_data_types",
        python_callable=validate_data_types,
    )
    
    

    
    
    # Скрипт: monitor_piplenes.py
# METADATA:
    # schedule: @hourly
    # start_date: today
    # description: Мониторинг статуса ETL пайплайнов
    # tasks: check_sales_pipeline, check_users_pipeline, send_alerts

    import datetime
    import random


    def check_sales_pipeline():
        """Проверка пайплайна продаж"""
        print("📊 Проверка пайплайна продаж...")
        status = random.choice(["SUCCESS", "RUNNING", "FAILED"])
        print(f"   Статус пайплайна 'sales_etl': {status}")

        if status == "FAILED":
            print("   ⚠️ Обнаружена проблема в пайплайне продаж!")

        return {"pipeline": "sales_etl", "status": status, "checked_at": str(datetime.datetime.now())}


    def check_users_pipeline():
        """Проверка пайплайна пользователей"""
        print("📊 Проверка пайплайна пользователей...")
        status = random.choice(["SUCCESS", "SUCCESS", "RUNNING"])  # Чаще успех
        print(f"   Статус пайплайна 'users_processing': {status}")
        return {"pipeline": "users_processing", "status": status, "checked_at": str(datetime.datetime.now())}


    def send_alerts():
        """Отправка алертов при проблемах"""
        print("📨 Проверка необходимости отправки алертов...")

        # Здесь в реальности была бы логика проверки статусов
        alerts_needed = random.choice([True, False])

        if alerts_needed:
            print("   🚨 Отправка алертов команде...")
            return {"alerts_sent": True, "recipients": ["team@company.com"], "message": "Обнаружены проблемы в пайплайнах"}
        else:
            print("   ✅ Все пайплайны в порядке, алерты не требуются")
            return {"alerts_sent": False, "message": "Все системы работают нормально"}


    print("Скрипт мониторинга пайплайнов инициализирован")

    
    monitor_check_sales_pipeline = PythonOperator(
        task_id="monitor_check_sales_pipeline",
        python_callable=check_sales_pipeline,
    )
    
    monitor_check_users_pipeline = PythonOperator(
        task_id="monitor_check_users_pipeline",
        python_callable=check_users_pipeline,
    )
    
    monitor_send_alerts = PythonOperator(
        task_id="monitor_send_alerts",
        python_callable=send_alerts,
    )
    
    

    
    
    # Скрипт: validation_data.sql
    
    validate_cmd_1 = SQLExecuteQueryOperator(
        task_id="validate_cmd_1",
        sql="""
SELECT
            'sales_data' as table_name,
            COUNT(*) as records_today,
            CASE
                WHEN COUNT(*) > 0 THEN 'OK'
                ELSE 'EMPTY_TABLE'
            END as status,
            CURRENT_DATE as check_date
        FROM sales
        WHERE sale_date = CURRENT_DATE;
""",
        conn_id="postgres_default",
        autocommit=True,
    )
    
    validate_cmd_2 = SQLExecuteQueryOperator(
        task_id="validate_cmd_2",
        sql="""
SELECT
            'users_data' as table_name,
            COUNT(*) as new_users_today,
            CASE
                WHEN COUNT(*) > 0 THEN 'OK'
                ELSE 'NO_NEW_USERS'
            END as status,
            CURRENT_DATE as check_date
        FROM users
        WHERE created_at::date = CURRENT_DATE;
""",
        conn_id="postgres_default",
        autocommit=True,
    )
    
    validate_cmd_3 = SQLExecuteQueryOperator(
        task_id="validate_cmd_3",
        sql="""
SELECT
            'validation_report' as report_type,
            JSON_BUILD_OBJECT(
                'validation_date', CURRENT_DATE,
                'sales_count', (SELECT COUNT(*) FROM sales WHERE sale_date = CURRENT_DATE),
                'users_count', (SELECT COUNT(*) FROM users WHERE created_at::date = CURRENT_DATE),
                'overall_status', CASE
                    WHEN (SELECT COUNT(*) FROM sales WHERE sale_date = CURRENT_DATE) > 0
                    AND (SELECT COUNT(*) FROM users WHERE created_at::date = CURRENT_DATE) > 0
                    THEN 'ALL_DATA_PRESENT'
                    ELSE 'MISSING_DATA'
                END
            ) as report_data;
""",
        conn_id="postgres_default",
        autocommit=True,
    )
    
    