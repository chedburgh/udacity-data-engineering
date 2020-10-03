import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP STAGING TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"

# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE STAGING TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist text,
    auth text,
    first_name text,
    gender text,
    items_in_session integer,
    last_name text,
    length float,
    level text,
    location text,
    method text,
    page text,
    registration float,
    session_id integer,
    song text,
    status integer,
    ts bigint,
    user_agent text,
    user_id integer
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id text NOT NULL,
    artist_latitude float,
    artist_longitude float,
    artist_location text,
    artist_name text NOT NULL,
    song_id text NOT NULL,
    title text NOT NULL,
    duration float,
    year integer
)
""")

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id integer identity(0,1),  
    user_id integer NOT NULL, 
    song_id text,
    artist_id text, 
    start_time bigint NOT NULL,    
    session_id integer NOT NULL, 
    level text NOT NULL, 
    location text NOT NULL, 
    user_agent text NOT NULL,
    PRIMARY KEY (songplay_id),
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (song_id) REFERENCES songs (song_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id), 
    FOREIGN KEY (start_time) REFERENCES time (start_time)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id integer NOT NULL, 
    first_name text, 
    last_name text, 
    gender text, 
    level text,
    PRIMARY KEY (user_id)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id text NOT NULL,
    artist_id text NOT NULL, 
    title text NOT NULL,  
    year integer, 
    duration float,
    PRIMARY KEY (song_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
  artist_id text NOT NULL, 
  name text NOT NULL, 
  location text, 
  latitude float, 
  longitude float,
  PRIMARY KEY (artist_id)
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp NOT NULL, 
    hour integer NOT NULL, 
    day integer NOT NULL, 
    week integer NOT NULL, 
    month integer NOT NULL, 
    year integer NOT NULL, 
    weekday integer NOT NULL,
    PRIMARY KEY (start_time)
)
""")

# COPY TO STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
    FROM {} 
    iam_role {} 
    region 'us-west-2' json {}
""").format(config["S3"]["LOG_DATA"], config["IAM_ROLE"]["ARN"], config["S3"]["LOG_JSONPATH"])

staging_songs_copy = ("""
COPY staging_songs 
    FROM {} 
    iam_role {} 
    region 'us-west-2'
    JSON 'auto' truncatecolumns
""").format(config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"])

# INSERT (STAGING -> FINAL)

songplay_table_insert = ("""
INSERT INTO songplays (
        user_id, 
        song_id, 
        artist_id, 
        start_time, 
        session_id, 
        level, 
        location, 
        user_agent)
    SELECT 
        events.user_id, 
        songs.song_id, 
        songs.artist_id, 
        events.ts, 
        events.session_id, 
        events.level, 
        events.location, 
        events.user_agent
    FROM staging_events events
    JOIN staging_songs songs 
        ON songs.artist_name = events.artist
        AND songs.title = events.song 
        AND songs.duration = events.length
    WHERE events.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level)
    SELECT 
        DISTINCT(user_id),
        first_name, 
        last_name, 
        gender, 
        level
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (
        song_id, 
        artist_id, 
        title, 
        year, 
        duration)
    SELECT 
        DISTINCT(song_id),
        artist_id,
        title,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists (
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude)
    SELECT
        DISTINCT(artist_id),
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (
        start_time,
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday)
    SELECT DISTINCT (start_time) as start_time,
       EXTRACT(hour FROM start_time) AS hour,
       EXTRACT(day FROM start_time) AS day,
       EXTRACT(week FROM start_time) AS week,
       EXTRACT(month FROM start_time) AS month,
       EXTRACT(year FROM start_time) AS year,
       EXTRACT(weekday FROM start_time) AS weekday
    FROM (
        SELECT DISTINCT ts,'1970-01-01'::date + ts/1000 * interval '1 second' as start_time
        FROM staging_events
        WHERE page = 'NextSong'
    )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
