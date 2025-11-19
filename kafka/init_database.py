def init_postgres():
    cursor.execute("""
        create table if not exists user_logins (
                   id serial prymary key,
                   username varchar(100),
                   event_type varchar(50),
                   event_time timestamp,
                   sent_to_kafka boolean default false
        )
    """)

    cursor.execute("""
        create table if not exists user_logins_kafka (
                   id integer prymary key,
                   username varchar(100),
                   event_type varchar(50),
                   event_time timestamp,
                   processed_at timestamp default current_timestamp
        )
    """)
    