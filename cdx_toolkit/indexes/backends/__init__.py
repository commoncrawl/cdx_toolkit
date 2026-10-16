'''
The index backends, one module each: the CDX API (cdx_api), CDX files (cdx_files), and the columnar URL index
queried with SQL (sql: common logic, sql_builder: the SQL text) via DuckDB (duckdb) or AWS Athena (athena).
'''
