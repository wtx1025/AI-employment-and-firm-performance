"""Merge per-year skill stats into an all-years table and a top-k view.

- Reads per-year outputs like: YYYY_skills_counts_co.parquet
- Aggregates counts across all years
- Exports:
    * all_years_skills_counts_co.parquet
    * top100_ai_skills_all_years.parquet 
    
Inputs:
(1) OUT_DIR: The path of the folder `out' in the USB
(2) TMP_DIR: The path of the folder `duckdb_tmp' in the USB
"""

import os 
import glob 
import duckdb 

OUT_DIR = r"E:\\out"          # Folder with per-year outputs (e.g., 2010_skills_counts_co.parquet)
TMP_DIR = r"E:\\duckdb_tmp"   # DuckDB spill/temp directory (ensure plenty of space; put on SSD/NvMe) 
MEM_LIMIT = "24GB"            # Memory cap for DuckDB
SAVE_AS = "parquet"           # Final output format: "parquet" or "csv" 
MIN_JOBS = 50                 # optional: filter out very rare skills in the global table 

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True) 


def norm(p: str) -> str:
    """Normalize Windows backslashes to forward slashes for DuckDB stability."""
    return os.path.abspath(p).replace("\\", "/")

out_dir = norm(OUT_DIR) 

parquet_files = sorted(
    glob.glob(f"{out_dir}/[0-9][0-9][0-9][0-9]_skills_counts_co.parquet")
)
csv_files = sorted(
    glob.glob(f"{out_dir}/[0-9][0-9][0-9][0-9]_skills_counts_co.csv")
)

# parquet_files = sorted(glob.glob(f"{out_dir}/201[0-8]_skills_counts_co.parquet"))
# csv_files     = sorted(glob.glob(f"{out_dir}/201[0-8]_skills_counts_co.csv"))

if parquet_files:
    # build a DuckDB list literal of file paths: ['path1', 'path2',...]
    # read_parquet([...], union_by_name=true) loads many files as one table 
    # (column-aligned by name)
    src_list  = "[" + ",".join(f"'{norm(p)}'" for p in parquet_files) + "]"
    read_src  = f"read_parquet({src_list}, union_by_name=true)"
    src_kind  = "parquet"
elif csv_files:
    # read_csv_auto([...], union_by_name=true) similarly unions many CSVs
    # auto-infers column types 
    src_list  = "[" + ",".join(f"'{norm(p)}'" for p in csv_files) + "]"
    read_src  = f"read_csv_auto({src_list}, union_by_name=true)"
    src_kind  = "csv"
else:
    raise FileNotFoundError(
        "No yearly files like YYYY_skills_counts_co.(parquet|csv) found in OUT_DIR."
    )

print(src_list)
  
copy_opts = (
    "(FORMAT 'parquet', COMPRESSION 'ZSTD')"
    if SAVE_AS == "parquet"
    else "(HEADER, DELIMITER ',')"
)
out_all = (
    f"{out_dir}/all_years_skills_counts_co."
    f"{'parquet' if SAVE_AS == 'parquet' else 'csv'}"
)  # Global table
out_top = (
    f"{out_dir}/top100_ai_skills_all_years."
    f"{'parquet' if SAVE_AS == 'parquet' else 'csv'}"
)  # Top-100 view (note: LIMIT 200 below)

# optional WHERE clause: keep only skills with sufficient global support 
# (total_cnt >= MIN_JOBS)
filter_clause = f"WHERE total_cnt >= {MIN_JOBS}" if MIN_JOBS > 0 else ""

con = duckdb.connect()
con.execute(f"PRAGMA threads={os.cpu_count()};")          # use all available CPU cores 
con.execute(f"PRAGMA memory_limit='{MEM_LIMIT}';")        # harp cap on memory usage
con.execute(f"PRAGMA temp_directory='{norm(TMP_DIR)}';")  # spill directory for large sorts/aggregations
con.execute("PRAGMA enable_progress_bar;")                # show progress bar during heavy queries 

# ----① Build the all-years table: skill, total_cnt, total_co, ai_score ----
# Pipeline:
#   all_years: read all yearly files as one table; normalize text and cast numeric columns
#   agg:       group by skill and sum counts across all years
#   filtered:  apply MIN_JOBS threshold (if any)
#   final:     compute ai_score = total_co / total_cnt; sort for convenience 
con.execute(f"""
COPY (
  WITH
  all_years AS (  
    SELECT
      lower(trim(skill))              AS skill, -- canonicalize skill keys across years
      CAST(cnt AS BIGINT)             AS cnt,   -- yearly total occurrences per skill
      CAST(co_jobs_with_ai AS BIGINT) AS co     -- yearly AI co-occurrence counts  
    FROM {read_src}
  ),
  agg AS (  -- aggregate over year
    SELECT
      skill,
      SUM(cnt) AS total_cnt,
      SUM(co)  AS total_co
    FROM all_years
    GROUP BY skill
  ),
  filtered AS ( 
    SELECT * FROM agg
    {filter_clause}
  )
  SELECT
    skill,
    total_cnt,
    total_co,
    CASE WHEN total_cnt > 0 THEN total_co * 1.0 / total_cnt ELSE 0 END AS ai_score
  FROM filtered
  ORDER BY ai_score DESC, total_cnt DESC, skill
) TO '{out_all}' {copy_opts};
""")

# ---- ② Produce a Top-100 file by ai_score (ties broken by total_cnt, then skill) ----
# We re-read the just written global table and slice the top 100 rows 
con.execute(f"""
COPY (
  SELECT *
  FROM read_{'parquet' if SAVE_AS=='parquet' else 'csv_auto'}('{out_all}')
  ORDER BY ai_score DESC, total_cnt DESC, skill
  LIMIT 100
) TO '{out_top}' {copy_opts};
""")

print(f"Done. Source kind: {src_kind}")
print(" - All years merged:", out_all)
print(" - Top 100 by ai_score:", out_top)