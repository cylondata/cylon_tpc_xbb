import pandas as pd 
import glob 
from pathlib import Path

DIR = "/N/u2/d/dnperera/romeo/bigbench"

print(glob.glob(f"{DIR}/*.parquet", recursive=True))

for path in Path(DIR).rglob('*.parquet'):
    if '-results' in str(path) and not path.is_dir(): 
        print('converting', path)
              
        df = pd.read_parquet(path)
        df.to_csv(f"{str(path.parent)}/{path.stem}.csv", index=False)

