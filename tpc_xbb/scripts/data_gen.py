import argparse
import os
import sys

parser = argparse.ArgumentParser(description='Process some integers.')

parser.add_argument('-s', '--scale', type=int, nargs='+',
                    help='scale in GBs', default=[1, 10, 100])
parser.add_argument('-p', '--parts', type=int, nargs='+',
                    help='queries', default=[1, 2, 4, 8])
parser.add_argument('--data_d', type=str, help='data dir',
                    default=f"{os.getenv('HOME')}/romeo/bigbench")
parser.add_argument('--pdgf_d', type=str, help='pdgf dir',
                    default=f"{os.getenv('HOME')}/romeo/git/TPCx-BB-kit-code-1.4.0/data-generator")

DATA_GEN_WORKERS = 1
JAVA_EXEC = '/usr/bin/java'


def main(_args):

    pdgf_jar = f"{_args['pdgf_d']}/pdgf.jar"

    for s in _args['scale']:

        for p in _args['parts']:
            print(f"generating scale:{s} partitions:{p}")

            for i in range(p):
                exec_str = f"{JAVA_EXEC} -jar {pdgf_jar} -nc {p} -nn {i} -ns -c -sp REFRESH_PHASE 0 -o "'$DATA_DIR/$PARTS/data/'+table.getName()+'/'" -workers $DATA_GEN_WORKERS -ap 3000 -s -sf $SCALE_F"

            res = os.system(exec_str)

            if res:
                print("ERROR exec:", exec_str)

            print(f"scale:{s} partitions:{p} done")
            print(f"=====================")


if __name__ == "__main__":
    args = parser.parse_args()
    args = vars(args)

    print("args:", args)
    main(args)
