"""Contains all SQL queries used within the application"""

DROP_SONGPLAYS_TABLE = "DROP TABLE IF EXISTS songplays;"
DROP_USERS_TABLE = "DROP TABLE IF EXISTS users;"
DROP_SONGS_TABLE = "DROP TABLE IF EXISTS songs;"
DROP_ARTISTS_TABLE = "DROP TABLE IF EXISTS artists;"
DROP_TIME_TABLE = "DROP TABLE IF EXISTS time;"
DROP_STAGING_SONGS_TABLE = "DROP TABLE IF EXISTS staging_songs;"
DROP_STAGING_EVENTS_TABLE = "DROP TABLE IF EXISTS staging_events;"

DROP_TABLE_STATEMENTS = [
    DROP_SONGPLAYS_TABLE,
    DROP_USERS_TABLE,
    DROP_SONGS_TABLE,
    DROP_ARTISTS_TABLE,
    DROP_TIME_TABLE,
    DROP_STAGING_SONGS_TABLE,
    DROP_STAGING_EVENTS_TABLE,
]

COPY_STAGING_SONGS = """
    COPY staging_songs
    FROM 's3://udacity-dend/song_data/'
    IAM_ROLE '{}'
    FORMAT AS JSON 'auto'
    REGION '{}';
"""

COPY_STAGING_LOGS = """
    COPY staging_logs
    FROM 's3://udacity-dend/log_data/'
    IAM_ROLE '{}'
    FORMAT AS JSON 's3://udacity-dend/log_json_path.json'
    REGION '{}';
"""

COPY_STATEMENTS = [COPY_STAGING_SONGS, COPY_STAGING_LOGS]

CREATE_STAGING_SONGS_TABLE = """
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INT
    )
    DISTSTYLE EVEN;
"""

CREATE_STAGING_EVENTS_TABLE = """
    CREATE TABLE IF NOT EXISTS staging_logs (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR(1),
        itemInSession INT,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        userAgent VARCHAR,
        userId INT
    )
    DISTSTYLE EVEN;
"""

CREATE_SONGPLAYS_TABLE = """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL SORTKEY,
        user_id INT NOT NULL,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
    )
    DISTSTYLE EVEN;
"""

CREATE_USERS_TABLE = """
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY SORTKEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR(1),
        level VARCHAR
    )
    DISTSTYLE ALL;
"""

CREATE_SONGS_TABLE = """
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY SORTKEY,
        title VARCHAR,
        artist_id VARCHAR,
        year INT,
        duration FLOAT
    )
    DISTSTYLE ALL;
"""

CREATE_ARTISTS_TABLE = """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY SORTKEY,
        name VARCHAR,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
    )
    DISTSTYLE ALL;
"""

CREATE_TIME_TABLE = """
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY SORTKEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    )
    DISTSTYLE ALL;
"""

CREATE_TABLE_STATEMENTS = [
    CREATE_STAGING_EVENTS_TABLE,
    CREATE_STAGING_SONGS_TABLE,
    CREATE_SONGPLAYS_TABLE,
    CREATE_ARTISTS_TABLE,
    CREATE_SONGS_TABLE,
    CREATE_TIME_TABLE,
    CREATE_USERS_TABLE,
]

INSERT_SONGPLAYS_TABLE = """
    INSERT INTO songplays (
        start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    )
    SELECT
        TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' AS start_time,
        se.userId AS user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId AS session_id,
        se.location,
        se.userAgent AS user_agent
    FROM staging_logs se
    LEFT JOIN staging_songs ss
        ON se.song = ss.title
        AND se.artist = ss.artist_name
    WHERE se.artist IS NOT NULL;
"""

INSERT_USERS_TABLE = """
    INSERT INTO users (
        user_id, first_name, last_name, gender, level
    )
    SELECT DISTINCT
        se.userId,
        se.firstName,
        se.lastName,
        se.gender,
        se.level
    FROM staging_logs se
    WHERE se.userId IS NOT NULL;
"""

INSERT_SONGS_TABLE = """
    INSERT INTO songs (
        song_id, title, artist_id, year, duration
    )
    SELECT DISTINCT
        ss.song_id,
        ss.title,
        ss.artist_id,
        ss.year,
        ss.duration
    FROM staging_songs ss
    WHERE ss.song_id IS NOT NULL;
"""

INSERT_ARTISTS_TABLE = """
    INSERT INTO artists (
        artist_id, name, location, latitude, longitude
    )
    SELECT DISTINCT
        ss.artist_id,
        ss.artist_name,
        ss.artist_location,
        ss.artist_latitude,
        ss.artist_longitude
    FROM staging_songs ss
    WHERE ss.artist_id IS NOT NULL;
"""

INSERT_TIME_TABLE = """
    INSERT INTO time (
        start_time, hour, day, week, month, year, weekday
    )
    SELECT DISTINCT
        TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' AS start_time,
        EXTRACT(hour FROM TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second') AS hour,
        EXTRACT(day FROM TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second') AS day,
        EXTRACT(week FROM TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second') AS week,
        EXTRACT(month FROM TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second') AS month,
        EXTRACT(year FROM TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second') AS year,
        EXTRACT(weekday FROM TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second') AS weekday
    FROM staging_logs se
    WHERE se.artist IS NOT NULL;
"""

INSERT_STATEMENTS = [
    INSERT_USERS_TABLE,
    INSERT_SONGS_TABLE,
    INSERT_ARTISTS_TABLE,
    INSERT_TIME_TABLE,
    INSERT_SONGPLAYS_TABLE,
]

TOP_PLAYED_SONGS_QUERY = """
    SELECT 
        s.title AS song_title,
        a.name AS artist_name,
        COUNT(sp.songplay_id) AS play_count
    FROM songplays sp
    JOIN songs s ON sp.song_id = s.song_id
    JOIN artists a ON sp.artist_id = a.artist_id
    GROUP BY s.title, a.name
    ORDER BY play_count DESC
    LIMIT 10;
"""

USER_LEVEL_DISTRIBUTION_QUERY = """
    SELECT 
        level,
        COUNT(DISTINCT user_id) AS user_count
    FROM users
    GROUP BY level
    ORDER BY user_count DESC;
"""

SAMPLE_QUERIES = [TOP_PLAYED_SONGS_QUERY, USER_LEVEL_DISTRIBUTION_QUERY]
