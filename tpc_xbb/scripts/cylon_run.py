import argparse
import os
import sys

parser = argparse.ArgumentParser(description='Process some integers.')

parser.add_argument('-w', '--workers', type=int, nargs='+',
                    help='workers', default=[1, 2, 4, 8])
parser.add_argument('-q', '--queries', type=str, nargs='+',
                    help='queries', default=["06", "07", "09", "14", "22", "23"])
parser.add_argument('--home_dir', type=str, help='cylon_tpc_home',
                    default=f"{os.getenv('HOME')}/romeo/git/cylon_tpc_xbb")


def replace_bench_config_yaml(src, dest, workers):
    with open(src, "rt") as fin:
        with open(dest, "wt") as fout:
            for line in fin:
                fout.write(line.replace('WWW', str(workers)))


def main(_args):
    python_exec = sys.executable
    cwd = os.getcwd()
    cylon_tpc_home = _args['home_dir']
    config_path = f"{cylon_tpc_home}/tpc_xbb/config/benchmark_config.yaml"

    for w in _args['workers']:
        replace_bench_config_yaml(f"{cwd}/cylon_benchmark_config.yaml.temp", config_path, w)

        for q in _args['queries']:
            print(f"query {q} starting")

            exec_str = f"mpirun -np {w} {python_exec} " \
                       f"{cylon_tpc_home}/tpc_xbb/queries/q{q}/tpc_xbb_query_{q}.py " \
                       f"--config_file={config_path}"

            res = os.system(exec_str)

            if res:
                print("ERROR exec:", exec_str)

            print(f"query {q} done")
            print(f"=====================")


if __name__ == "__main__":
    args = parser.parse_args()
    args = vars(args)

    print("args:", args)
    main(args)
