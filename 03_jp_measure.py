"""Compute per-job AI scores for a given year and derive firm-level AI shares.

Steps:
1) Read all postings for YEAR, explode skills, map to global ai_score per skill.
2) Average per job to get job_ai_score; flag ai_job (threshold = 0.1).
3) Aggregate to company-level ai_job share and export. 
"""

import os 
import glob 
import duckdb 

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

YEAR = 2024
SAVE_AS = "parquet"
ROOT_DIR = r"E:\\jobs_by_year"
SUBDIR = "parquet"
COL_NAME = "skills_name" 
JOB_ID = "id"
COMPANY = "company" 
COMPANY_NAME = "company_name" 

OUT_DIR   = r"E:\\out"        
TMP_DIR   = r"E:\\duckdb_tmp" 
MEM_LIMIT = "24GB"

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True) 


def norm(p: str) -> str:
    """Normalize Windows path separators to forward slashes for DuckDB."""
    return os.path.abspath(p).replace("\\", "/")

year_pattern = f"{norm(ROOT_DIR)}/{YEAR}/{SUBDIR}/*.parquet"
if not glob.glob(year_pattern):
    raise FileNotFoundError(f"cannot find any files:{year_pattern}")

all_parquet = f"{norm(OUT_DIR)}/all_years_skills_counts_co.parquet"
all_csv     = f"{norm(OUT_DIR)}/all_years_skills_counts_co.csv"

if os.path.exists(all_parquet):
    skill_src_fn = f"read_parquet('{all_parquet}')"
elif os.path.exists(all_csv):
    skill_src_fn = f"read_csv_auto('{all_csv}')"
else:
    raise FileNotFoundError("cannot find all_years_skills_counts_co.(parquet|csv), please generate the file first")

out_jobs = (
    f"{norm(OUT_DIR)}/{YEAR}_job_ai_scores."
    f"{'parquet' if SAVE_AS=='parquet' else 'csv'}"
)
copy_opts = (
    "(FORMAT 'parquet', COMPRESSION 'ZSTD')"
    if SAVE_AS == "parquet"
    else "(HEADER, DELIMITER ',')"
)

# ---------------------------------------------------------------------------
# Connect to DuckDB
# ---------------------------------------------------------------------------

con = duckdb.connect()
con.execute(f"PRAGMA threads={os.cpu_count()};")
con.execute(f"PRAGMA memory_limit='{MEM_LIMIT}';")
con.execute(f"PRAGMA temp_directory='{norm(TMP_DIR)}';")
con.execute("PRAGMA enable_progress_bar;")

# ------------------------------------------------------------------------------------------
# 1) Compute per-job AI score 
# ------------------------------------------------------------------------------------------

sql = f"""
COPY (
  WITH
  base AS (
    SELECT
      CAST({JOB_ID} AS VARCHAR) AS job_id,
      CAST({COMPANY} AS VARCHAR) AS company,
      CAST({COMPANY_NAME} AS VARCHAR)  AS company_name,
      CAST({COL_NAME} AS VARCHAR) AS skills
    FROM read_parquet('{year_pattern}', union_by_name=true, hive_partitioning=0)
    WHERE {COL_NAME} IS NOT NULL AND {COL_NAME} <> ''
  ),
  exploded AS (
    SELECT
      job_id,
      company,
      company_name,
      lower(trim(x)) AS skill
    FROM base, UNNEST(str_split(skills, '|')) AS t(x)
    WHERE x IS NOT NULL AND x <> ''
  ),
  -- De-duplicate to avoid counting the same (job_id, skill) more than once
  -- If you want repeated occurrences of the same skill within a job to count,
  -- remove this CTE and join from `exploded` directly in the next step. 
  dedup AS (
    SELECT DISTINCT job_id, company, company_name, skill
    FROM exploded
  ),
  joined AS (
    SELECT
      d.job_id,
      d.company,
      d.company_name, 
      s.ai_score
    FROM dedup d
    LEFT JOIN {skill_src_fn} s
      ON d.skill = s.skill
  ),
  per_job AS (
    SELECT
      job_id,
      company,
      company_name,
      AVG(ai_score)           AS job_ai_score,    -- Use AVG(COALESCE(ai_score, 0)) if you want unmatched skills to contribute 0 instead of NULL
      COUNT(*)                AS n_skills,        -- Number of distinct skills for the job 
      COUNT(ai_score)         AS n_matched_skills -- Skills that matched an ai_score 
    FROM joined
    GROUP BY job_id, company, company_name
  )
  SELECT
    job_id,
    company,
    company_name,
    job_ai_score,
    n_skills,
    n_matched_skills,
    CASE WHEN job_ai_score > 0.1 THEN 1 ELSE 0 END AS ai_job
  FROM per_job
  -- Keep only jobs with at least one matched skill by uncommenting the line below 
  -- WHERE n_matched_skills > 0
  ORDER BY job_ai_score DESC NULLS LAST, n_matched_skills DESC, job_id
) TO '{out_jobs}' {copy_opts};
"""
con.execute(sql)

read_fn = f"read_{'parquet' if SAVE_AS=='parquet' else 'csv_auto'}"
n = con.execute(f"SELECT COUNT(*) FROM {read_fn}('{out_jobs}')").fetchone()[0]
print("DONE.")
print(f" - Per-job AI scores saved to: {out_jobs}")
print(f" - Rows: {n}")

# ------------------------------------------------------------------------------------------
# 2) Aggregate to company-year level  
# ------------------------------------------------------------------------------------------

per_job_rel = f"{read_fn}('{out_jobs}')"
sql_company_share = f"""
COPY (
  SELECT
    company_name,
    COUNT(*)                       AS n_postings,
    SUM(ai_job)                    AS n_ai_jobs,
    AVG(CAST(ai_job AS DOUBLE))    AS ai_job_share  
  FROM {per_job_rel}
  GROUP BY company_name
  ORDER BY ai_job_share DESC NULLS LAST, n_postings DESC, company_name
) TO '{norm(OUT_DIR)}/{YEAR}_company_ai_share.parquet' (FORMAT 'parquet', COMPRESSION 'ZSTD');
"""
con.execute(sql_company_share)
print("Successfully saved the calculated measures in the given year!")