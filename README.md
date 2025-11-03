# Data Engineering Internship Projects

Практические проекты по инженерии данных, демонстрирующие различные технологии и подходы к работе с данными.

## 📂 Проекты

### 🐘 [PostgreSQL Audit](./postgress_audit/)
**Транзакционная система аудита пользователей (OLTP)**

Система для отслеживания и аудита операций пользователей с использованием PostgreSQL. Включает триггеры, логирование изменений и отчетность по активности.

**Технологии**: PostgreSQL, триггеры, функции аудита

### 🚀 [ClickHouse Events Analytics](./clickhouse_events/)
**Аналитическая система событий с расчетом Retention (OLAP)**

OLAP-решение для анализа пользовательских событий с автоматической агрегацией данных через Materialized Views. Расчет метрик Retention и поведенческой аналитики.

**Технологии**: ClickHouse, Materialized Views, State/Merge функции, AggregatingMergeTree
## 🎯 Цель проектов

Демонстрация полного цикла работы с данными:

| Этап | Технология | Назначение |
|------|------------|------------|
| **Транзакции** | PostgreSQL | Операционные данные, аудит |
| **Аналитика** | ClickHouse | Агрегация, метрики, Retention |
| **Потоки** | Kafka | Реал-тайм обработка событий |
| **Оркестрация** | Airflow | Управление пайплайнами |

## 🚀 Быстрый старт

```bash
# PostgreSQL Audit
cd postgress_audit
psql -f postgress_audit_data.sql

# ClickHouse Analytics
cd clickhouse_events
clickhouse-client < user_events_analytics.sql
```


