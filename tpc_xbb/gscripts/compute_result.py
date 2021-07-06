import pandas as pd
import sys
import glob

if len(sys.argv) < 2:
	raise ValueError("number of workers has to be given as the first argument to the script")
nworkers=int(sys.argv[1])
print("number of workers:", nworkers)

file_pattern = "single_run_*.csv"
files = glob.glob(file_pattern)
df_list = []
for file in files:
	df1 = pd.read_csv(file, names=["rank", "compute_time"])
	df_list.append(df1)

df = pd.concat(df_list)
df = df.set_index("rank").sort_index()
print(df)
print("number of worker results: ", len(df.index))
if len(df.index) != nworkers:
	raise ValueError("Number of worker results does not match the number of workers.")

avg = df["compute_time"].mean()

f = open("results.csv", "a")
f.write("%.0f" % avg)
f.write("\n")
f.close()

print("average compute time:", "%.0f" % avg, "ms")
