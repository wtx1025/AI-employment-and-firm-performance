"""Merge profile and description parquet files using DuckDB

This script reads all parquet files from the `profile` and `description`
folders, performs a left join on `id` (keeping all profile records),
filters out rows with missing `most_recent_company_start_month`,
`company_name`, or `description_raw`, and writes the result to a single
`merged.parquet` file, with a progress bar enabled.
"""

import duckdb

con = duckdb.connect()
con.execute("PRAGMA enable_progress_bar = true;")

con.execute("""
COPY (
    SELECT
        id,
        p.company,
        p.company_name,
        p.title_raw,
        p.most_recent_company_start_month,
        p.most_recent_company_end_month,
        d.description_raw
    FROM read_parquet('D:/profile/*.parquet') AS p
    LEFT JOIN read_parquet('D:/description/*.parquet') AS d
    USING (id)
    WHERE p.most_recent_company_start_month IS NOT NULL
      AND p.company_name IS NOT NULL
      AND d.description_raw IS NOT NULL
) TO 'D:/merged.parquet' (FORMAT PARQUET);
""")