
-- Содаем табличку для сырых данных: движок Mergtree, сотрировка по дата и id

create table user_events
(
    user_id UInt32,
    event_type String,
    spent_points UInt32,
    event_date DateTime
)
engine = MergeTree()
partition by toYYYYMM(event_date)
order by (event_date, user_id)
ttl event_date + interval 30 day;

-- Создание таблички для агрегированных данных

create table user_events_aggregated
(
    event_date Date,
    event_type String,
    total_users AggregateFunction(uniq, UInt32),
    total_spent_points AggregateFunction(sum, UInt32),
    total_actions AggregateFunction(count, UInt32)
)
engine = AggregatingMergeTree()
partition by toYYYYMM(event_date)
order by (event_date, event_type)
ttl event_date + interval 180 day;

-- Создаем материализованное представление
create materialized view user_events_mv
to user_events_aggregated
as
select
    toDate(event_date) as event_date,
    event_type,
    uniqState(user_id) as total_users,
    sumState(spent_points) as total_spent_points,
    countState() as total_actions
from user_events
group by event_date, event_type;



-- вставляем данные 
insert into user_events values
(1, 'login', 0, now() - interval 10 day),
(2, 'signup', 0, now() - interval 10 day),
(3, 'login', 0, now() - interval 10 day),
(1, 'login', 0, now() - interval 7 day),
(2, 'login', 0, now() - interval 7 day),
(3, 'purchase', 30, now() - interval 7 day),
(1, 'purchase', 50, now() - interval 5 day),
(2, 'logout', 0, now() - interval 5 day),
(4, 'login', 0, now() - interval 5 day),
(1, 'login', 0, now() - interval 3 day),
(3, 'purchase', 70, now() - interval 3 day),
(5, 'signup', 0, now() - interval 3 day),
(2, 'purchase', 20, now() - interval 1 day),
(4, 'logout', 0, now() - interval 1 day),
(5, 'login', 0, now() - interval 1 day),
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());

-- проверям наполнение таблицы 
select * from user_events ue 

-- запрос для аналитики по дням

select 
	event_date,
	event_type, uniqMerge(total_users) as uniq_users, 
	sumMerge(total_spent_points) as total_spent,
	countMerge(total_actions) as actions_count
from user_events_aggregated
group by event_date, event_type 
order by event_date desc, event_type;


-- Расчитаем retention

create temporary table users_daily as 
select 
	toDate(event_date) as date,
	uniq(user_id) as daily_uniq_users,
	groupUniqArray(user_id) as ussser_array
from user_events
group by date;

select 
    d1.date as day_0,
    d1.daily_uniq_users as total_users_day_0,
    arrayIntersect(d1.ussser_array, d2.ussser_array ) as returned_users,
    length(returned_users) as returned_in_7_days,
    round(returned_in_7_days * 100.0 / total_users_day_0, 2) as retention_7d_percent,
    format('{0}|{1}|{2}%', 
        total_users_day_0, 
        returned_in_7_days, 
        round(returned_in_7_days * 100.0 / total_users_day_0, 2)
    ) as result_format
from users_daily d1
left join users_daily d2 on d2.date = d1.date + interval 7 day
where d1.date <= now() - interval 7 day
order by day_0;


-- смотрим статистику
select
    uniqMerge(total_users) as total_unique_users,
    sumMerge(total_spent_points) as overall_spent_points,
    countMerge(total_actions) as overall_actions
from user_events_aggregated;


	

