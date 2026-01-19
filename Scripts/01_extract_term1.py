 """Build per-year skill co-occurrence counts from Parquet files using DuckDB
  
Reads all Parquet files for a given YEAR under ROOT_DIR/YEAR/SUBDIR,
explodes the pipe-separated skill list, computes per-skill counts and the
co-occurrence with predefined AI terms, and writes a single output file
(CSV or Parquet) to OUT_DIR. 

Inputs: 
(1) YEAR: Users are required to run through 2010-2024, one year at a time
(2) ROOT_DIR: The path of the folder `jobs_by_year' in the USB
(3) OUT_DIR: The path of the folder `out' in the USB
(4) TMP_DIR: The path of the folder `duckdb_tmp' in the USB 
"""

import glob 
import os 
import duckdb

YEAR = 2024
SAVE_AS = "parquet"   # choose to save the file as 'csv' or 'parquet' 

ROOT_DIR = r"E:\\jobs_by_year"         # root folder containing year-level data 
SUBDIR   = "parquet"                   # subfolder under yearly data folder 
COL_NAME = r"skills_name"              # columns that contains the skills list 
JOB_ID   = "id"                        # unique identifier for a job   

OUT_DIR  = r"E:\\out"                  # output folder
TMP_DIR  = r"E:\\duckdb_tmp"           # DuckDB spill/temp directory (fast disk)
MEM_LIMIT = "24GB"                     # memory cap; spills beyond this limit  

# Ensure output/temp folders exist  
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(TMP_DIR,  exist_ok=True) 


def norm(p: str) -> str:
  """Normalize a Windows path to forward slashes for DuckDB stability"""
    return os.path.abspath(p).replace("\\", "/")

pattern = f"{norm(ROOT_DIR)}/{YEAR}/{SUBDIR}/*.parquet"  # Ex: D:/jobs_by_year/2010/parquet/*.parquet
# Make sure there are files for the given year 
if not glob.glob(pattern):
    raise FileNotFoundError(f"Cannot find any files : {pattern}")

# Decide output filename and DuckDB COPY options based on SAVE_AS  
out_all = (
    f"{norm(OUT_DIR)}/{YEAR}_skills_counts_co."
    f"{'parquet' if SAVE_AS == 'parquet' else 'csv'}"
)
copy_opts = (
    "(FORMAT 'parquet', COMPRESSION 'ZSTD')"
    if SAVE_AS == "parquet"
    else "(HEADER, DELIMITER ',')"
)

# Establish DuckDB connection and resource settings 
con = duckdb.connect() 
con.execute(f"PRAGMA threads={os.cpu_count()};")          # use all CPU cores 
con.execute(f"PRAGMA memory_limit='{MEM_LIMIT}';")        # memory cap 
con.execute(f"PRAGMA temp_directory='{norm(TMP_DIR)}';")  # spill directory 
con.execute("PRAGMA enable_progress_bar;")                # show progress bar  

# Combined Parquet and compute:
# - cnt: number of jobs containing the skills (per year) 
# - co_jobs_with_ai: among those, number of jobs that include any AI core term   
con.execute(f"""
COPY (
  WITH
  base AS (
    SELECT CAST({JOB_ID} AS VARCHAR) AS job_id,           
           CAST({COL_NAME} AS VARCHAR) AS skills
    FROM read_parquet('{pattern}', union_by_name=true, hive_partitioning=0) -- Read all parquet files in the folder for the specified year 
    WHERE {COL_NAME} IS NOT NULL AND {COL_NAME} <> ''                       --Filter out columns with all cells empty 
  ),
  exploded AS (                                                             -- job1 : 'A|B|C' => job1:'a', job1:'b', job1:'c' 
    SELECT job_id, lower(trim(x)) AS skill                                  -- Standardize to prevent 'Python' and 'python' being split into two separate entries 
    FROM base, UNNEST(str_split(skills, '|')) AS t(x)                       -- Expand the array into multiple rows
    WHERE x IS NOT NULL AND x <> ''
  ),
  ai_terms(term) AS (  -- Create a constant table with only one column named 'term', listing AI core words we define
    VALUES
      ('ai'),
      ('artificial intelligence'),
      ('ml'),
      ('machine learning'),
      ('nlp'),
      ('natural language processing'),
      ('cv'),
      ('computer vision')
  ),
  ai_jobs AS (                                      -- Extract the columns containing skills belonging to the AI vocabulary list from the exploded data, retaining their job_id
    SELECT DISTINCT job_id                          -- For the same job, even if there are multiple AI terms, only keep one entry to avoid duplicate counting later 
    FROM exploded
    WHERE skill IN (SELECT term FROM ai_terms)
  )
  
  -- Final aggregation: for each normalized skill, output 
  --    -cnt: total number of job rows that contain this skill in the year 
  --    -co_jobs_with_ai: number of those rows whose job_id is an AI job (co-occurrence with any AI core word)
  -- Assumptions:
  --    * Each (job_id, skill) pair appears at most once (no duplicate same-skill tokens per job)
  --    * ai_jobs contains DISTINCT job_ids that have at least one AI core word 
  
  SELECT
    e.skill,
    COUNT(*)::BIGINT AS cnt,                                -- jobs containing this skill (since job_id+skill is unique)
    SUM(CASE WHEN aj.job_id IS NOT NULL
             THEN 1 ELSE 0 END)::BIGINT AS co_jobs_with_ai  -- among those, jobs that are AI jobs (co-occurrence count)
  FROM exploded e
  LEFT JOIN ai_jobs aj USING (job_id)                       -- keep non-AI jobs too; ai.job_id is NULL for them => adds 0 
  -- WHERE e.skill NOT IN (SELECT term FROM ai_terms)       -- (optional) exclude AI terms themselves from the output
  GROUP BY e.skill                                          -- aggregate per skill 
  ORDER BY co_jobs_with_ai DESC, cnt DESC, e.skill          -- sort by co-occurrence, then total count, then skill name 
) TO '{out_all}' {copy_opts};
""")

print("DONE. single file output：", out_all)