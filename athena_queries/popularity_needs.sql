SELECT
    corr(CAST(track_popularity AS DOUBLE), CAST(artist_popularity AS DOUBLE)) AS corr_artist,
    corr(CAST(track_popularity AS DOUBLE), CAST(album_popularity AS DOUBLE)) AS corr_album,
    corr(CAST(track_popularity AS DOUBLE), CAST(duration_sec AS DOUBLE)) AS corr_duration
FROM datawarehouse
WHERE 
    track_popularity IS NOT NULL AND
    artist_popularity IS NOT NULL AND
    album_popularity IS NOT NULL AND
    duration_sec IS NOT NULL;
