class SqlQueries:

    staging_events_table_create= ("""
        create table if not exists staging_events (
            artist varchar,
            auth varchar,
            firstName varchar,
            gender varchar,
            itemInSession integer,
            lastName varchar,
            length decimal,
            level varchar,
            location varchar,
            method varchar,
            page varchar,
            registration varchar,
            sessionId integer,
            song varchar,
            status varchar,
            ts bigint,
            userAgent varchar,
            userId integer
        )
    """)

    staging_songs_table_create = ("""
        create table if not exists staging_songs (
            song_id varchar PRIMARY KEY,
            title varchar,
            duration decimal,
            year integer,
            num_songs integer,
            artist_id varchar,
            artist_name varchar,
            artist_latitude decimal,
            artist_longitude decimal,
            artist_location varchar
        )
    """)

    songplays_table_create = ("""
        create table if not exists songplays (
            songplay_id integer PRIMARY KEY IDENTITY(0,1),
            start_time timestamp NOT NULL,
            user_id varchar NOT NULL default '',
            level varchar NOT NULL default '',
            song_id varchar NOT NULL default '',
            artist_id varchar NOT NULL default '',
            session_id varchar NOT NULL default '',
            location varchar NOT NULL default '',
            user_agent varchar NOT NULL default ''
        )
    """)

    users_table_create = ("""
        create table if not exists users (
            user_id varchar PRIMARY KEY,
            first_name varchar NOT NULL,
            last_name varchar NOT NULL,
            gender varchar(1) NOT NULL,
            level varchar NOT NULL
        )
    """)

    artists_table_create = ("""
        create table if not exists artists (
            artist_id varchar PRIMARY KEY,
            artist_name varchar NOT NULL,
            artist_location varchar NOT NULL,
            artist_latitude decimal,
            artist_longitude decimal
        )
    """)

    songs_table_create = ("""
        create table if not exists songs (
            song_id varchar PRIMARY KEY,
            title varchar NOT NULL,
            artist_id varchar NOT NULL,
            year integer NOT NULL,
            duration decimal NOT NULL
        )
    """)

    time_table_create = ("""
         create table if not exists time (
            start_time timestamp PRIMARY KEY,
            hour integer NOT NULL,
            day integer NOT NULL,
            week integer NOT NULL,
            month integer NOT NULL,
            year integer NOT NULL,
            weekday integer NOT NULL
         )
    """)

    songplay_table_insert = ("""
        insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT
                events.start_time, 
                events.userid as user_id,
                events.level,
                songs.song_id, 
                songs.artist_id, 
                events.sessionid as session_id, 
                events.location, 
                events.useragent as user_agent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong' and userid is not null) events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
            where songs.song_id is not null
    """)

    users_table_insert = ("""
        insert into users (user_id, first_name, last_name, gender, level)
        SELECT distinct userid as user_id, firstname as first_name, lastname as last_name, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    songs_table_insert = ("""
        insert into songs (song_id, title, artist_id, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artists_table_insert = ("""
        insert into artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        insert into time (start_time, hour, day, week, month, year, weekday)
        SELECT
            distinct (timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second') as start_time,
            DATE_PART(hour, timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second') as hour,
            DATE_PART(day, timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second') as day,
            DATE_PART(week, timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second') as week,
            DATE_PART(month, timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second') as month,
            DATE_PART(year, timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second') as year,
            DATE_PART(weekday, timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second') as weekday
        FROM staging_events
    """)