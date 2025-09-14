import os, glob, duckdb 

YEAR = 2025
SAVE_AS = "parquet"   # choose to save the file as 'csv' or 'parquet' 

ROOT_DIR = r"D:\\jobs_by_year" # this is the file that includes all year level data 
SUBDIR   = "parquet" # subfolder under yearly data folder 
COL_NAME = r"specialized_skills_name"   # we extract the skills from this column 
JOB_ID   = "id"                         # this is the column that identify different job  

OUT_DIR  = r"D:\\out" # we put the output data in this folder
TMP_DIR  = r"D:\\duckdb_tmp" # DuckDB spill temp directory (needs lots of free space; put it on SSD/NVMe)
MEM_LIMIT = "24GB" # memory limit, our RAM is 32 GB, it is reasonable to set MEM_LIMIT as 24 GB 

# if the output folder or temporary folder does not exist we build one 
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(TMP_DIR,  exist_ok=True) 

# Convert Windows backslashes to '/', as DuckDB is more stable with this format
def norm(p: str) -> str:
    return os.path.abspath(p).replace("\\", "/")

pattern = f"{norm(ROOT_DIR)}/{YEAR}/{SUBDIR}/*.parquet"  # Ex: D:/jobs_by_year/2010/parquet/*.parquet
# use glob function to check whether there are files in the given year 
if not glob.glob(pattern):
    raise FileNotFoundError(f"Cannot find any files : {pattern}")

# Decide the output filename and COPY options (switch based on SAVE_AS)
# We use copy_opts to tell DuckDB what format to write the query results in, and which export options to apply 
out_all   = f"{norm(OUT_DIR)}/{YEAR}_skills_counts_co.{'parquet' if SAVE_AS=='parquet' else 'csv'}"
copy_opts = "(FORMAT 'parquet', COMPRESSION 'ZSTD')" if SAVE_AS == "parquet" else "(HEADER, DELIMITER ',')"

# Establish DuckDB connection and resource settings 
con = duckdb.connect() 
con.execute(f"PRAGMA threads={os.cpu_count()};") # Parallel worker threads: use all available CPU cores 
con.execute(f"PRAGMA memory_limit='{MEM_LIMIT}';") # Set memory cap; spill to TMP_DIR when exceeded 
con.execute(f"PRAGMA temp_directory='{norm(TMP_DIR)}';") # Set spill/temp directory 
con.execute("PRAGMA enable_progress_bar;") # Show progress bar (switch to disable for quiet mode) 

# In the following query, we combine all the parquet files and calculate two things for each unique skill
# cnt: Number of jobs with this skill in the given year
# co_jobs_with_ai: Number of times this skill appears in jobs containing any AI core words 
con.execute(f"""
COPY (
  WITH
  base AS (
    SELECT CAST({JOB_ID} AS VARCHAR) AS job_id,           
           CAST({COL_NAME} AS VARCHAR) AS skills
    FROM read_parquet('{pattern}', union_by_name=true, hive_partitioning=0) -- Read all parquet files in the folder for the specified year 
    WHERE {COL_NAME} IS NOT NULL AND {COL_NAME} <> '' --Filter out columns with all cells empty 
  ),
  exploded AS (                                     -- job1 : 'A|B|C' => job1:'a', job1:'b', job1:'c' 
    SELECT job_id, lower(trim(x)) AS skill            -- Standardize to prevent 'Python' and 'python' being split into two separate entries 
    FROM base, UNNEST(str_split(skills, '|')) AS t(x) -- Expand the array into multiple rows
    WHERE x IS NOT NULL AND x <> ''
  ),
  ai_terms(term) AS (                               -- Create a constant table with only one column named 'term', listing AI core words we define
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
    COUNT(*)::BIGINT AS cnt,                        -- jobs containing this skill (since job_id+skill is unique)
    SUM(CASE WHEN aj.job_id IS NOT NULL
             THEN 1 ELSE 0 END)::BIGINT AS co_jobs_with_ai  -- among those, jobs that are AI jobs (co-occurrence count)
  FROM exploded e
  LEFT JOIN ai_jobs aj USING (job_id)               -- keep non-AI jobs too; ai.job_id is NULL for them => adds 0 
  -- WHERE e.skill NOT IN (SELECT term FROM ai_terms) -- (optional) exclude AI terms themselves from the output
  GROUP BY e.skill                                    -- aggregate per skill 
  ORDER BY co_jobs_with_ai DESC, cnt DESC, e.skill    -- sort by co-occurrence, then total count, then skill name 
) TO '{out_all}' {copy_opts};
""")

print("DONE. single file output：", out_all)

