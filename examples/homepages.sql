-- The homepages of the .va domain in one crawl: successful captures of the path "/" without a query string.
--
--   cdxt index cc-url-index-s3 --sql examples/homepages.sql --yes -o homepages.csv
--   cdxt repackage homepages.csv --prefix HOMEPAGES
--
-- --sql runs the query as it is, so it names the Parquet files it reads (here one crawl of the columnar URL
-- index on S3, which needs AWS credentials); INDEX (cc-url-index-s3) only selects DuckDB. cdxt repackage needs
-- the columns url, warc_filename, warc_record_offset and warc_record_length.
SELECT url, fetch_time, fetch_status, content_mime_type,
       warc_filename, warc_record_offset, warc_record_length
FROM read_parquet('s3://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2024-33/subset=warc/*.parquet')
WHERE url_host_tld = 'va'
  AND url_path = '/'
  AND url_query IS NULL
  AND fetch_status = 200
