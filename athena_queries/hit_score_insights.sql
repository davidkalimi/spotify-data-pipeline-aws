CREATE OR REPLACE VIEW hit_score_insights AS
SELECT 
    track_name,
    artist_id,
    genre,
    label,
    release_date,
    CAST(duration_sec AS DOUBLE) AS duration_sec,
    CAST(track_popularity AS DOUBLE) AS track_popularity,
    CAST(artist_popularity AS DOUBLE) AS artist_popularity,
    CAST(album_popularity AS DOUBLE) AS album_popularity,

 --calculation
    (
        0.4 * CAST(track_popularity AS DOUBLE) +
        0.3 * CAST(artist_popularity AS DOUBLE) +
        0.2 * CAST(album_popularity AS DOUBLE) +
        0.1 * (1 - ABS(CAST(duration_sec AS DOUBLE) - 180) / 180) * 100
    ) AS hit_score

FROM datawarehouse
WHERE 
    track_popularity IS NOT NULL AND 
    artist_popularity IS NOT NULL AND 
    album_popularity IS NOT NULL AND 
    duration_sec IS NOT NULL;
