import pandas as pd
import sys

if len(sys.argv) < 2:
	raise ValueError("number of workers has to be given as the first argument to the script")
nworkers=int(sys.argv[1])
print("number of workers:", nworkers)

df = pd.read_csv("single_run_results.csv", names=["worker_rank", "compute_time"])
clmn = df.loc[:, "compute_time"]
print(df)
print("number of worker results: ", len(clmn))
if len(clmn) != nworkers:
	raise ValueError("Number of worker results does not match the number of workers.")
avg = clmn.mean()

f = open("results.csv", "a")
f.write("%.2f" % avg)
f.write("\n")
f.close()

print("average compute time:", "%.2f" % avg, "seconds")
