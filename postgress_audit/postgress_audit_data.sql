
-- 1. Создаем таблицы
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

CREATE TABLE export_history (
    id SERIAL PRIMARY KEY,
    file_path TEXT,
    rows_exported INTEGER,
    status TEXT,
    exported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2.Создание функции логировани по трем полям: name, email, role
create or replace function log_user_audit()
returns trigger as $$
begin
    
    if old.name is distinct from new.name then
        insert into users_audit (user_id, changed_by, field_changed, old_value, new_value)
        values (old.id, current_user, 'name', old.name, new.name);
    end if;
    
    -- логируем изменения email
    if old.email is distinct from new.email then
        insert into users_audit (user_id, changed_by, field_changed, old_value, new_value)
        values (old.id, current_user, 'email', old.email, new.email);
    end if;
    
    -- логируем изменения роли
    if old.role is distinct from new.role then
        insert into users_audit (user_id, changed_by, field_changed, old_value, new_value)
        values (old.id, current_user, 'role', old.role, new.role);
    end if;

    return new;
end;
$$ language plpgsql;

-- 3.Содание тригера
create trigger trigger_user_audit 
    before update on users
    for each row
    execute procedure log_user_audit();


--4. Наполняем таблицу данными
INSERT INTO users (name, email, role) VALUES 
('Иван Иванов', 'ivan@test.com', 'user'),
('Петр Петров', 'petr@test.com', 'admin'),
('Мария Сидорова', 'maria@test.com', 'user');


--проверка таблиц
select * from users;
select * from users_audit; 
select *from export_history;

--тестируем данные на обновление данных
UPDATE users SET name = 'Иван Сидоров' WHERE id = 1;

UPDATE users SET role = 'manager' WHERE id = 2;

UPDATE users SET email = 'ivan.sidorov@newmail.com' WHERE id = 1;

update users set
	name = 'Петр Николаев',
	email = 'petr.nicko@company.com',
	role = 'director'
where id = 2;

--5. Создаем расширение pg_cron
create extension if not exists pg_cron;



-- Настраиваем запуск выгрузки файла в 3:00 ночи по москве, поставила 0:00 так время контейнера и постгреса отличается на 3 часа
select cron.schedule(
    'nightly-audit-export',       -
    '0 0 * * *',                  -- (3:00 по москве)
    'SELECT * FROM export_yesterdays_audit_with_log();' 

--проверяем что задание планировщика установлено
select jobid, jobname, schedule, command from cron.job;
    
-- просмотр заданий планировщика
select * from cron.job;


 
-- Создаем функцию экспорта за вчерашний день. Выгрузка будет сохранять файлы в контейнер докера, как временное хранение
create or replace function export_yesterdays_audit_with_log()
returns text as $$
declare
    file_path text;
    rows_count integer;
begin
    file_path := '/tmp/users_audit_export_' || to_char(current_date - interval '1 day', 'yyyymmdd') || '.csv';
    
    execute 'copy (
        select 
            ua.id,
            ua.user_id,
            u.name as user_name,
            ua.changed_at,
            ua.changed_by,
            ua.field_changed,
            ua.old_value,
            ua.new_value
        from users_audit ua
        left join users u on ua.user_id = u.id
        where ua.changed_at::date = current_date - interval ''1 day''
        order by ua.changed_at
    ) to ''' || file_path || ''' with csv header';
    
    get diagnostics rows_count = row_count;
    
    insert into export_history (file_path, rows_exported, status) 
    values (file_path, rows_count, 'success');
    
    return 'экспортировано ' || rows_count || ' строк в ' || file_path;
end;
$$ language plpgsql;

--запускаем функцию экспорта
select * from export_yesterdays_audit_with_log();

--проверяем таблицу экспорта
select * from export_history;

 
 
 UPDATE users SET name = 'Ручной тест ' || NOW()::TIME WHERE id = 1;
 
 SELECT * FROM users_audit ORDER BY changed_at DESC LIMIT 5;
 
 SELECT * FROM export_yesterdays_audit_with_log();
 
 SELECT * FROM export_history ORDER BY exported_at DESC;
 

--Проверяем таблицы
select * from users_audit; 
select * from users;
select * from export_history;

-- Выполните в PostgreSQL:
\i postgres_audit_system.sql




