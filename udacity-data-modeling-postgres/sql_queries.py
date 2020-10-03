# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# DROP STAGING TABLES

songplay_staging_drop = "DROP TABLE IF EXISTS songplays_staging"
time_staging_drop = "DROP TABLE IF EXISTS time_staging"
user_staging_drop = "DROP TABLE IF EXISTS users_staging"

# CREATE TABLES

# The data is poor quality, we have to remove the artist/song id
# as NOT NULL since most have no entries in the artist/song tables
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id serial,  
    user_id integer NOT NULL, 
    song_id text,
    artist_id text, 
    start_time bigint NOT NULL,    
    session_id integer NOT NULL, 
    level text NOT NULL, 
    location text NOT NULL, 
    user_agent text NOT NULL,
    PRIMARY KEY (songplay_id)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id integer NOT NULL, 
    first_name text, 
    last_name text, 
    gender text, 
    level text,
    PRIMARY KEY (user_id)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id text NOT NULL,
    artist_id text NOT NULL, 
    title text NOT NULL,  
    year integer, 
    duration float8,
    PRIMARY KEY (song_id)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
  artist_id text NOT NULL, 
  name text NOT NULL, 
  location text, 
  latitude float8, 
  longitude float8,
  PRIMARY KEY (artist_id)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time bigint NOT NULL, 
    hour integer NOT NULL, 
    day integer NOT NULL, 
    week integer NOT NULL, 
    month integer NOT NULL, 
    year integer NOT NULL, 
    weekday integer NOT NULL,
    PRIMARY KEY (start_time)
);
""")

# STAGING TABLES

songplay_staging = "songplays_staging"
user_staging = "users_staging"
time_staging = "time_staging"

songplay_staging_create = ("""
CREATE TEMP TABLE IF NOT EXISTS songplays_staging (
    user_id integer NOT NULL, 
    start_time bigint NOT NULL,    
    session_id integer NOT NULL, 
    level text, 
    location text, 
    user_agent text,
    song text,
    artist text,
    duration float8
);
""")

user_staging_create = ("""
CREATE TEMP TABLE IF NOT EXISTS users_staging (
    user_id integer NOT NULL, 
    first_name text, 
    last_name text, 
    gender text, 
    level text
);
""")

time_staging_create = ("""
CREATE TEMP TABLE IF NOT EXISTS time_staging (
    start_time bigint NOT NULL, 
    hour integer NOT NULL, 
    day integer NOT NULL, 
    week integer NOT NULL, 
    month integer NOT NULL, 
    year integer NOT NULL, 
    weekday integer NOT NULL
);
""")

# STAGING INSERTS

songplay_insert_from_staging = ("""
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
        sp.user_id, 
        s.song_id, 
        a.artist_id, 
        sp.start_time, 
        sp.session_id, 
        sp.level, 
        sp.location, 
        sp.user_agent
    FROM songplays_staging sp
    LEFT JOIN artists a ON a.name = sp.artist
    LEFT JOIN songs s ON s.title = sp.song AND s.duration = sp.duration;
""")

user_insert_from_staging = ("""
INSERT INTO users (
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level)
    SELECT 
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    FROM users_staging
    ON CONFLICT DO NOTHING;
""")

time_insert_from_staging = ("""
INSERT INTO time (
        start_time, 
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday)
    SELECT * FROM time_staging
    ON CONFLICT DO NOTHING;
""")

# INSERT RECORDS

song_table_insert = ("""
INSERT INTO songs (
    song_id, 
    artist_id, 
    title, 
    year, 
    duration) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

# Unused
# songplay_table_insert = ("""
# INSERT INTO users (
#    songplay_id,  
#    user_id, 
#    song_id, 
#    artist_id, 
#    session_id, 
#    start_time,
#    level text, 
#    location, 
#    user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
# """)

# Unused
# user_table_insert = ("""
# INSERT INTO users (
#    user_id, 
#    first_name, 
#    last_name, 
#    gender,
#    level) VALUES (%s, %s, %s, %s, %s)
#    ON CONFLICT DO NOTHING;
# """)

# Unused
# time_table_insert = ("""
# INSERT INTO time (
#    start_time, 
#    hour, 
#    day, 
#    week,
#    month, 
#    year,
#    weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)
#    ON CONFLICT DO NOTHING;
# """)

# FIND SONGS

# Unused
# song_select = ("""
#    SELECT s.song_id, a.artist_id
#    FROM songs s
#    JOIN artists a ON s.artist_id = a.artist_id
#    WHERE s.title = %s AND a.name = %s AND s.duration = %s;
# """)

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]