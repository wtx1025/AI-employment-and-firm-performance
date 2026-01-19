"""Build year-expanded resume records, detect AI-related profiles using
a keyword list, and aggregate to company-year AI measures

Uses lowercased matching to make detection case-insensitive.

Inputs:
1) INPUT_PATH: The path of file `' in the USB, this is the main file that contains
   resume data
2) KEYWORDS_PARQUET:
3) OUT_PARQUET: The path of the output file that contains the results after expanding
   each employment spell into one row per year
4) OUT_PARQUET_FLAG: The path of the output file that contains the results after marking 
   each row as ai-related or non-ai-related
5) OUT_PARQUET_COMP: The path of the final output file, which contains firm-year AI measure
"""

import duckdb 
import os 

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

INPUT_PATH = r"E:\\merged.parquet"  
KEYWORDS_PARQUET = r"E:\\top100_ai_skills_all_years.parquet"
OUT_PARQUET = r"E:\\temp.parquet"
OUT_PARQUET_FLAG = r"E:\\temp1.parquet"
OUT_PARQUET_COMP = r"E:\\temp2.parquet"

TABLE_NAME = "resume" 
DEST_TABLE = "resume_years"

COL_ID = "id"
COL_START = "most_recent_company_start_month"
COL_END = "most_recent_company_end_month"
COL_TITLE = "title_raw"
COL_COMPANY = "company_name"
COL_DESC = "description_raw"

# ---------------------------------------------------------------------------
# Connect to DuckDB
# ---------------------------------------------------------------------------

con = duckdb.connect() 
con.execute("PRAGMA temp_directory='C:\\\\duckdb_temp';")           
con.execute("PRAGMA memory_limit='20GB';")    
con.execute("PRAGMA enable_progress_bar=true;")

# ---------------------------------------------------------------------------
# 1) Load raw CSV into a normalized table with parsed dates and flags
# ---------------------------------------------------------------------------

print("Break point 0")
con.execute(f"""
    CREATE OR REPLACE TABLE {TABLE_NAME} AS
    WITH raw AS (
        SELECT
            CAST({COL_ID} AS VARCHAR) AS id,
            {COL_TITLE}   AS title_name,
            {COL_COMPANY} AS company_name_raw,
            {COL_DESC}    AS description_raw,
            NULLIF(TRIM({COL_START}), '') AS start_txt,
            NULLIF(TRIM({COL_END}),   '') AS end_txt
        FROM read_parquet('{INPUT_PATH}')
    )
    SELECT
        id, title_name, company_name_raw, description_raw,
        CAST(try_strptime(start_txt || '-01', '%Y-%m-%d') AS DATE) AS job_start_ym,
        CAST(try_strptime(end_txt   || '-01', '%Y-%m-%d') AS DATE) AS job_end_ym
    FROM raw
""")

print("Break point 1")

# ---------------------------------------------------------------------------
# 2) Expand each employment spell into one row per year (inclusive)
#    - Drop rows with missing end AND is_current = 0
#    - Guard against end < start using GREATEST
# ---------------------------------------------------------------------------

con.execute(f"""
    CREATE OR REPLACE TABLE {DEST_TABLE} AS
    WITH base AS (
        SELECT
            id, title_name, company_name_raw, description_raw, job_start_ym, job_end_ym,
            year(job_start_ym) AS sy,
            CASE
              WHEN job_end_ym IS NOT NULL THEN year(job_end_ym)
              ELSE year(current_date()) 
            END AS ey_pre
        FROM {TABLE_NAME}
        WHERE job_start_ym IS NOT NULL
    ),
    filtered AS (
        -- Keep only rows with a resolvable end year (incl. current jobs).
        SELECT
            id, title_name, company_name_raw, description_raw, sy,
            GREATEST(ey_pre, sy) AS ey 
        FROM base
        WHERE ey_pre IS NOT NULL
    )
    SELECT
        id,
        title_name,
        company_name_raw,
        description_raw,
        y AS year
    FROM filtered
    CROSS JOIN range(sy, ey + 1) AS t(y)   -- Inclusive of end year 
    ORDER BY id, year
""")

con.execute(f"""
  COPY (SELECT * FROM resume_years ORDER BY id, title_name, year)
  TO '{OUT_PARQUET}'
  (FORMAT PARQUET);
""")

print("Break point 2")

# ---------------------------------------------------------------------------
# 3) Load AI keyword list from parquet and normalize
# ---------------------------------------------------------------------------

con.execute(f"""
    CREATE TABLE ai_keywords AS
    SELECT DISTINCT LOWER(TRIM(skill)) AS skill
    FROM read_parquet('{KEYWORDS_PARQUET}')
    WHERE skill IS NOT NULL AND TRIM(skill) <> '';
""")

# ---------------------------------------------------------------------------
# 4) Build a lowercase concatenated text field for simple substring matching
#    - text_all = lower(title_name) + ' ' + lower(description_raw or '')
# ---------------------------------------------------------------------------

con.execute("""
    CREATE VIEW resume_years_text AS
    SELECT
        id,
        title_name,
        company_name_raw,
        description_raw,
        "year",
        LOWER(COALESCE(title_name, '')) || ' ' || LOWER(COALESCE(description_raw, '')) AS text_all
    FROM resume_years
""")

# ---------------------------------------------------------------------------
# 5) For each row, select the first matching keyword by earliest position
#    - first_hit_skill is NULL when no keyword matches
# ---------------------------------------------------------------------------

con.execute("""
    CREATE OR REPLACE VIEW resume_years_ai_first AS
    SELECT
        r.*,
        (
            SELECT k.skill
            FROM ai_keywords k
            WHERE POSITION(k.skill IN r.text_all) > 0
            ORDER BY POSITION(k.skill IN r.text_all)  
            LIMIT 1
        ) AS first_hit_skill
    FROM resume_years_text r
""")

# ---------------------------------------------------------------------------
# 6) Flag AI-related rows using the presence of first_hit_skill
# ---------------------------------------------------------------------------

con.execute("""
    CREATE OR REPLACE TABLE resume_years_ai AS
    SELECT
        id,
        title_name,
        company_name_raw,
        description_raw,
        "year",
        CASE WHEN first_hit_skill IS NOT NULL THEN 1 ELSE 0 END AS ai_related,
        first_hit_skill
    FROM resume_years_ai_first
    ORDER BY id, title_name, "year";
""")

# ---------------------------------------------------------------------------
# 7) Aggregate to company-year: unique persons as denominator, AI persons as
#    numerator, then compute ai_measure = ai_employees / employees
#    - Use (company_name_raw, year, id) to avoid double-counting titles
# ---------------------------------------------------------------------------

con.execute("""
    CREATE OR REPLACE VIEW company_year_person AS
    SELECT
        LOWER(TRIM(company_name_raw)) AS company_key,  -- Standardized key
        company_name_raw              AS company_name, -- Display label 
        "year",
        id,
        MAX(ai_related) AS ai_related_any  -- Any AI hit per person 
    FROM resume_years_ai
    GROUP BY 1, 2, 3, 4
""")

con.execute("""
    CREATE OR REPLACE TABLE company_year_ai AS
    SELECT
        company_name,
        "year",
        COUNT(*) AS employees,                 -- Unique persons 
        SUM(ai_related_any) AS ai_employees,   -- Persons with AI hit
        CASE WHEN COUNT(*) > 0
             THEN CAST(SUM(ai_related_any) AS DOUBLE) / COUNT(*)
             ELSE NULL
        END AS ai_measure
    FROM company_year_person
    GROUP BY company_name, "year"
    ORDER BY company_name, "year";
""")

# ---------------------------------------------------------------------------
# 8) Persist outputs
# ---------------------------------------------------------------------------

con.execute(f"""
  COPY (SELECT * FROM resume_years_ai)
  TO '{OUT_PARQUET_FLAG}' (FORMAT PARQUET);
""")

con.execute(f"""
  COPY (SELECT * FROM company_year_ai)
  TO '{OUT_PARQUET_COMP}' (FORMAT PARQUET);
""")