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