-- METADATA:
-- schedule: @daily
-- start_date: today
-- description: Валидация загрузки новых данных за сегодня
-- tasks: check_sales_data, check_users_data, generate_report

-- Задача 1: Проверка данных продаж за сегодня
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

-- Задача 2: Проверка новых пользователей за сегодня
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

-- Задача 3: Генерация сводного отчёта
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