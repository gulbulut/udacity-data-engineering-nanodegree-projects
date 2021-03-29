# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (songplay_id SMALLSERIAL PRIMARY KEY, 
start_time timestamp NOT NULL,
user_id int NOT NULL, 
level varchar NOT NULL, 
song_id varchar, 
artist_id varchar, 
session_id int NOT NULL, 
location varchar NOT NULL, 
user_agent varchar NOT NULL
);
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS  users (
user_id int PRIMARY KEY, 
first_name varchar NOT NULL, 
last_name varchar, 
gender varchar(1), 
level varchar NOT NULL
);
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (
song_id varchar PRIMARY KEY, 
title varchar NOT NULL, 
artist_id varchar NOT NULL, 
year int NOT NULL, 
duration float NOT NULL
);
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists ( 
artist_id varchar PRIMARY KEY, 
name varchar NOT NULL, 
location varchar NULL, 
latitude float  NULL,
longitude float NULL
);
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
start_time timestamp PRIMARY KEY, 
hour int NOT NULL, 
day int NOT NULL, 
week int NOT NULL, 
month int NOT NULL, 
year int NOT NULL, 
weekday int NOT NULL
);
""")
# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
values (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
values (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) 
DO UPDATE SET level=EXCLUDED.level
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
values (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
values (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
""")


time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday)
values (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
""")

# FIND SONGS

song_select = ("""SELECT s.song_id, s.artist_id 
    FROM artists AS a
    JOIN songs AS s
    ON s.artist_id = a.artist_id
    WHERE s.title = %s AND a.name = %s AND s.duration = %s;""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]