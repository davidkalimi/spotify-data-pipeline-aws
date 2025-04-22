# 🎛️ AWS Glue Visual Job – Spotify Track Join & Cleanup

This job was built using AWS Glue Studio's Visual ETL Editor (no-code).

## 📌 Goal:
Join raw Spotify datasets (track, album, artist) and produce a clean dataset for Athena and QuickSight.

## 🧩 Pipeline Steps:

1. **Sources from S3:**
   - `track.csv` → `s3://project-soptify-datewithdata/staging/`
   - `album.csv` → `s3://project-soptify-datewithdata/staging/`
   - `artist.csv` → `s3://project-soptify-datewithdata/staging/`

2. **Join Logic:**
   - Join `album` with `artist` on `artist_id`
   - Join result with `track` on `track_id`

3. **Transformations:**
   - Dropped columns: `.track_id`, `id`
   - Output is cleaned and optimized for BI

4. **Destination:**
   - Output written to:
     `s3://project-soptify-datewithdata/datawarehouse/`
   - Format: Parquet, compressed with Snappy

5. **Data Quality:**
   - Used `ColumnCount > 0` as a basic rule
   - Evaluated via `EvaluateDataQuality()` function

6. **Execution:**
   - Job can be triggered manually or scheduled via AWS Glue

## 📷 Visual ETL Flow:

See: `diagrams/glue_job_flow.png`
