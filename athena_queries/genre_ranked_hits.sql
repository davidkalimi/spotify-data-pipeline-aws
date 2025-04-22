CREATE OR REPLACE VIEW genre_ranked_hits AS
SELECT
    track_id,
    track_name,
    artist_id,
    genre,
    label,
    release_date,
    CAST(duration_sec AS DOUBLE) AS duration_sec,
    CAST(track_popularity AS DOUBLE) AS track_popularity,
    CAST(artist_popularity AS DOUBLE) AS artist_popularity,
    CAST(album_popularity AS DOUBLE) AS album_popularity,

    -- Compute HIT SCORE (same formula as before)
    (
        0.4 * CAST(track_popularity AS DOUBLE) +
        0.3 * CAST(artist_popularity AS DOUBLE) +
        0.2 * CAST(album_popularity AS DOUBLE) +
        0.1 * (1 - ABS(CAST(duration_sec AS DOUBLE) - 180) / 180) * 100
    ) AS hit_score,

    -- Rank of the track within its genre based on hit score
    RANK() OVER (PARTITION BY genre ORDER BY 
        (
            0.4 * CAST(track_popularity AS DOUBLE) +
            0.3 * CAST(artist_popularity AS DOUBLE) +
            0.2 * CAST(album_popularity AS DOUBLE) +
            0.1 * (1 - ABS(CAST(duration_sec AS DOUBLE) - 180) / 180) * 100
        ) DESC
    ) AS genre_rank,

    -- Row number of the track name (to identify duplicates)
    ROW_NUMBER() OVER (PARTITION BY track_name ORDER BY release_date DESC) AS track_name_instance,

    -- Average hit score per genre
    AVG(
        0.4 * CAST(track_popularity AS DOUBLE) +
        0.3 * CAST(artist_popularity AS DOUBLE) +
        0.2 * CAST(album_popularity AS DOUBLE) +
        0.1 * (1 - ABS(CAST(duration_sec AS DOUBLE) - 180) / 180) * 100
    ) OVER (PARTITION BY genre) AS genre_avg_hit_score

FROM datawarehouse
WHERE 
    genre IS NOT NULL AND
    track_popularity IS NOT NULL AND 
    artist_popularity IS NOT NULL AND 
    album_popularity IS NOT NULL AND 
    duration_sec IS NOT NULL;
