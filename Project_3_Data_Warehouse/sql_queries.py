import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplays_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
record_id int IDENTITY(0,1) PRIMARY KEY,
artist varchar,
auth varchar,
firstName varchar,
gender varchar(1),
itemInSession int,
lastName varchar,
length float,
level varchar,
location varchar,
method varchar,
page varchar,
registration varchar,
sessionId int,
song varchar,
status int,
ts timestamp,
userAgent varchar,
userId int
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
num_songs int,
artist_id varchar,
artist_latitude float,
artist_longitude float,
artist_location varchar,
artist_name varchar,
song_id varchar PRIMARY KEY,
title varchar,
duration float,
year int
)
""")

songplays_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id int IDENTITY(0,1) PRIMARY KEY,
start_time timestamp not null SORTKEY DISTKEY,
user_id int not null,
level varchar,
song_id varchar,
artist_id varchar,
session_id int,
location varchar,
user_agent varchar
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id int SORTKEY PRIMARY KEY,
first_name varchar,
last_name varchar,
gender varchar(1),
level varchar
)
diststyle ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id varchar SORTKEY PRIMARY KEY,
title varchar,
artist_id varchar not null,
year int,
duration float
)
diststyle ALL;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id varchar SORTKEY,
name varchar,
location varchar,
latitude float,
longitude float,
primary key (artist_id)
)
diststyle ALL;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time timestamp DISTKEY SORTKEY,
hour int,
day int,
week int,
month int,
year int,
weekday int,
primary key (start_time)
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY stg_events
FROM {}
IAM_ROLE {}
TIMEFORMAT as 'epochmillisecs'
TRUNCATECOLUMNS
BLANKSASNULL
EMPTYASNULL
COMPUPDATE OFF
REGION 'us-west-2'
FORMAT AS json {};
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY stg_songs
FROM {}
IAM_ROLE {}
TIMEFORMAT as 'epochmillisecs'
TRUNCATECOLUMNS
BLANKSASNULL
EMPTYASNULL
COMPUPDATE OFF
REGION 'us-west-2'
FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_INSERT = ("""
INSERT into songplay (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
select
events.ts start_time,
events.userId user_id,
events.level,
songs.song_id,
songs.artist_id,
events.sessionId session_id,
events.location,
events.userAgent user_agent
from stg_events events inner join stg_songs songs
on events.song=songs.title
and events.artist=songs.artist_name
where events.page='NextSong'
""")

user_table_INSERT = ("""
INSERT into users (user_id,first_name,last_name,gender,level)
select distinct
userId,
firstName,
lastName,
gender,
level
from stg_events
where UserId is not null
and page='NextSong'
""")

song_table_INSERT = ("""
INSERT into songs (song_id,title,artist_id,year,duration)
select distinct
song_id,
title,
artist_id,
year,
duration
from stg_songs
where song_id is not null
""")

artist_table_INSERT= ("""
INSERT into artists (artist_id,name,location,latitude,longitude)
select distinct
artist_id,
artist_name,
artist_location,
artist_latitude,f
artist_longitude
from stg_songs
where artist_id is not null
""")

time_table_INSERT = ("""
INSERT into time (start_time,hour,day,week,month,year,weekday)
select distinct
ts start_time,
extract(hour from ts),
extract(day from ts),
extract(week from ts),
extract(month from ts),
extract(year from ts),
extract(weekday from ts)
from stg_events
where ts is not null
and page='NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]