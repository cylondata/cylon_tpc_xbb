import argparse
import os
import sys

parser = argparse.ArgumentParser(description='Process some integers.')

parser.add_argument('-w', '--workers', type=int, nargs='+',
                    help='workers', default=[1, 2, 4, 8])
parser.add_argument('-q', '--queries', type=int, nargs='+',
                    help='queries', default=[6, 7, 9, 14, 22, 23])
parser.add_argument('--home_dir', type=str, help='cylon_tpc_home',
                    default=f"{os.getenv('HOME')}/victor/git/cylon_tpc_xbb")
parser.add_argument('--mpi_params', type=str, help='mpi params',
                    default="-mca btl vader,tcp,openib,self \
                            -mca btl_tcp_if_include enp175s0f0 \
                            --mca btl_openib_allow_ib 1 \
                            --map-by node --bind-to core --bind-to socket")
parser.add_argument('--config_template', type=str, help='config template path',
                    default=None)


# mpi_params = "-mca btl vader,tcp,openib,self \
#             -mca btl_tcp_if_include enp175s0f0 \
#             --mca btl_openib_allow_ib 1 \
#             --map-by node --bind-to core --bind-to socket" #--report-bindings


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

    config_template_path = _args['config_template'] if _args['config_template'] is not None \
        else f"{cylon_tpc_home}/tpc_xbb/scripts/cylon_benchmark_config.yaml.temp"

    print("python_exec:", python_exec)
    print("config_path:", config_path)
    print("config_template_path:", config_template_path)

    for w in _args['workers']:
        replace_bench_config_yaml(config_template_path, config_path, w)

        for q in _args['queries']:
            print(f"query {q} starting")

            exec_str = f"mpirun {_args['mpi_params']} -np {w} {python_exec} " \
                       f"{cylon_tpc_home}/tpc_xbb/queries/q{q:02}/tpcx_bb_query_{q:02}.py " \
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
