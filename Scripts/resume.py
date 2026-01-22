import duckdb 
import os 

INPUT_PATH = r"C:\\Users\\王亭烜\\Downloads\\us_profiles_samples_full.csv"
TABLE_NAME = "resume" 
DEST_TABLE = "resume_years"

COL_ID = "ID"
COL_START = "JOB_START_YM"
COL_END = "JOB_END_YM"
COL_TITLE = "TITLE_NAME"
COL_COMPANY = "COMPANY_NAME_RAW"
COL_DESC = "DESCRIPTION_RAW"
COL_CURRENT = "IS_CURRENT"

con = duckdb.connect() 

con.execute(f"""
    CREATE OR REPLACE TABLE {TABLE_NAME} AS
    WITH raw AS (
        SELECT
            CAST({COL_ID} AS VARCHAR) AS id,
            {COL_TITLE}   AS title_name,
            {COL_COMPANY} AS company_name_raw,
            {COL_DESC}    AS description_raw,
            NULLIF(TRIM({COL_START}), '') AS start_txt,
            NULLIF(TRIM({COL_END}),   '') AS end_txt,
            NULLIF(TRIM({COL_CURRENT}), '') AS is_current_txt
        FROM read_csv_auto('{INPUT_PATH}',
                           header=true,
                           sample_size=-1,
                           all_varchar=true)
    )
    SELECT
        id, title_name, company_name_raw, description_raw,
        CAST(try_strptime(start_txt || '-01', '%Y-%m-%d') AS DATE) AS job_start_ym,
        CAST(try_strptime(end_txt   || '-01', '%Y-%m-%d') AS DATE) AS job_end_ym,
        COALESCE(TRY_CAST(is_current_txt AS INTEGER), 0) AS is_current
    FROM raw
""")

con.execute(f"""
    CREATE OR REPLACE TABLE {DEST_TABLE} AS
    WITH base AS (
        SELECT
            id, title_name, company_name_raw, description_raw, job_start_ym, job_end_ym, is_current,
            year(job_start_ym) AS sy,
            CASE
              WHEN job_end_ym IS NOT NULL THEN year(job_end_ym)
              WHEN is_current = 1           THEN year(current_date())
              ELSE NULL
            END AS ey_pre
        FROM {TABLE_NAME}
        WHERE job_start_ym IS NOT NULL
    ),
    filtered AS (
        -- Drop observations where end is missing and is_current=0 (ey_pre is NULL)
        SELECT
            id, title_name, company_name_raw, description_raw, sy,
            GREATEST(ey_pre, sy) AS ey  -- Prevent end < start; rows with NULL ey_pre won't enter here 
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
    CROSS JOIN range(sy, ey + 1) AS t(y)   -- Inclusive of the ending year 
    ORDER BY id, year
""")

OUT_PARQUET = r"C:\\Users\\王亭烜\\Desktop\\RA\\Konan\\temp.parquet"
con.execute(f"""
  COPY (SELECT * FROM resume_years ORDER BY id, title_name, year)
  TO '{OUT_PARQUET}'
  (FORMAT PARQUET);
""")

#=======================================================================

KEYWORDS_PARQUET = r"E:\\out\\top100_ai_skills_all_years.parquet"

con.execute(f"""
    CREATE TABLE ai_keywords AS
    SELECT DISTINCT LOWER(TRIM(skill)) AS skill
    FROM read_parquet('{KEYWORDS_PARQUET}')
    WHERE skill IS NOT NULL AND TRIM(skill) <> '';
""")

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

con.execute("""
    CREATE OR REPLACE VIEW resume_years_ai_first AS
    SELECT
        r.*,
        (
            SELECT k.skill
            FROM ai_keywords k
            WHERE POSITION(k.skill IN r.text_all) > 0
            ORDER BY POSITION(k.skill IN r.text_all)  -- The earliest (left-most) match 
            LIMIT 1
        ) AS first_hit_skill
    FROM resume_years_text r
""")

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

#=============================================================================

con.execute("""
    -- First collapse to one row per year (company, year, person) to avoid double-counting
    -- when the same person has multiple roles/titles in the same company-year 
    CREATE OR REPLACE VIEW company_year_person AS
    SELECT
        LOWER(TRIM(company_name_raw)) AS company_key,  -- Can be used as a standardized key
        company_name_raw              AS company_name, -- Keep the raw name for display 
        "year",
        id,
        MAX(ai_related) AS ai_related_any  -- Whether this person has any AI hit in that year (at least once)
    FROM resume_years_ai
    GROUP BY 1, 2, 3, 4
""");

con.execute("""
    CREATE OR REPLACE TABLE company_year_ai AS
    SELECT
        company_name,
        "year",
        COUNT(*) AS employees,  -- Already one row per person 
        SUM(ai_related_any) AS ai_employees,
        CASE WHEN COUNT(*) > 0
             THEN CAST(SUM(ai_related_any) AS DOUBLE) / COUNT(*)
             ELSE NULL
        END AS ai_measure
    FROM company_year_person
    GROUP BY company_name, "year"
    ORDER BY company_name, "year";
""")

OUT_PARQUET_FLAG = r"C:\\Users\\王亭烜\\Desktop\\RA\\Konan\\temp1.parquet"
con.execute(f"""
  COPY (SELECT * FROM resume_years_ai)
  TO '{OUT_PARQUET_FLAG}' (FORMAT PARQUET);
""")

OUT_PARQUET_COMP = r"C:\\Users\\王亭烜\\Desktop\\RA\\Konan\\temp2.parquet"
con.execute(f"""
  COPY (SELECT * FROM company_year_ai)
  TO '{OUT_PARQUET_COMP}' (FORMAT PARQUET);
""")