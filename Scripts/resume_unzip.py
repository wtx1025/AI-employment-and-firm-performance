import pandas as pd
from pathlib import Path

for folder in [r"D:\Profile"]:
    for csv_path in Path(folder).glob("*.csv"):
        df = pd.read_csv(csv_path)
        df.to_parquet(csv_path.with_suffix(".parquet"), index=False)