CREATE OR REPLACE DATABASE AWS_S3_LOAD;

CREATE OR REPLACE SCHEMA AWS_S3_PIPELINE;

CREATE OR REPLACE STORAGE INTEGRATION s3_init
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::126623035604:role/s3_snowflake_conn'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-project-sannu')

DESC INTEGRATION s3_init;

CREATE OR REPLACE FILE FORMAT csv_fileformat
    TYPE = csv
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE STAGE AWS_spotify_stage
    URL='s3://spotify-etl-project-sannu/transformed_data/'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = csv_fileformat;

CREATE OR REPLACE TABLE album (
    album_id VARCHAR(255) PRIMARY KEY,
    name_ VARCHAR(255),
    release_date DATE,
    total_tracks INT,
    url_ VARCHAR(255)
);

CREATE OR REPLACE TABLE artist (
    artist_id VARCHAR(255) PRIMARY KEY,
    artist_name VARCHAR(255),
    external_url VARCHAR(255)
);

CREATE OR REPLACE TABLE songs (
    song_id VARCHAR(255),
    song_name VARCHAR(255),
    duration_ms INT,
    url_ VARCHAR(255),
    popularity INT,
    song_added TIMESTAMP_TZ,
    album_id VARCHAR(255),
    artist_id VARCHAR(255),
    rank INT,                            
    scrape_date DATE,                    

    PRIMARY KEY (song_id, scrape_date),
    FOREIGN KEY (album_id) REFERENCES album(album_id),
    FOREIGN KEY (artist_id) REFERENCES artist(artist_id)
);

CREATE OR REPLACE PIPE AWS_spotify_album_pipe
AUTO_INGEST = True
AS
COPY INTO album
FROM @AWS_spotify_stage/album/;

CREATE OR REPLACE PIPE AWS_spotify_artist_pipe
AUTO_INGEST = True
AS
COPY INTO artist
FROM @AWS_spotify_stage/artist/;

CREATE OR REPLACE PIPE AWS_spotify_songs_pipe
AUTO_INGEST = True
AS
COPY INTO songs
FROM @AWS_spotify_stage/songs/;

SHOW pipes;

DESC pipe AWS_spotify_album_pipe;

DESC pipe AWS_spotify_artist_pipe;

DESC pipe AWS_spotify_songs_pipe;

SELECT count(*) FROM album;

SELECT count(*) FROM artist;

SELECT count(*) FROM songs;


-- Spotify Analysis Queries
-- Top 10 trending songs in last 7 days
SELECT song_id, song_name, artist_id, album_id, rank, scrape_date
FROM songs
WHERE scrape_date >= CURRENT_DATE - INTERVAL '7' DAY
ORDER BY rank ASC
LIMIT 10;

-- Track album popularity over time
SELECT 
    s.album_id,
    a.album_name,
    s.scrape_date,
    AVG(s.rank) AS avg_rank
FROM songs s
JOIN album a ON s.album_id = a.album_id
GROUP BY s.album_id, a.album_name, s.scrape_date
ORDER BY s.album_id, s.scrape_date;

-- Artists with most top-10 entries
SELECT 
    artist_id,
    COUNT(*) AS top_10_appearances
FROM songs
WHERE rank <= 10
GROUP BY artist_id
ORDER BY top_10_appearances DESC
LIMIT 10;

-- Daily chart movement of a song
SELECT 
    scrape_date,
    rank
FROM songs
WHERE song_id = '<song_id_here>'
ORDER BY scrape_date;