#
# Copyright (c) 2019-2020, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# use query 6

import sys

from typing import Iterable
import cupy
import time

from cylon_xbb_tools.utils import (
    tpcxbb_argparser,
)
from cylon_xbb_tools.cudf_readers import CSVReader

import pygcylon as gcy


def read_tables(env: gcy.CylonEnv, config) -> Iterable[gcy.DataFrame]:
    table_reader = CSVReader(config["data_dir"], rank=None if env.world_size == 1 else env.rank, nworkers=env.world_size)

    web_sales_cols = [
        "ws_bill_customer_sk",
        "ws_sold_date_sk",
        "ws_ext_list_price",
        "ws_ext_wholesale_cost",
        "ws_ext_discount_amt",
        "ws_ext_sales_price",
    ]
    store_sales_cols = [
        "ss_customer_sk",
        "ss_sold_date_sk",
        "ss_ext_list_price",
        "ss_ext_wholesale_cost",
        "ss_ext_discount_amt",
        "ss_ext_sales_price",
    ]
    date_cols = ["d_date_sk", "d_year", "d_moy"]
    customer_cols = [
        "c_customer_sk",
        "c_customer_id",
        "c_email_address",
        "c_first_name",
        "c_last_name",
        "c_preferred_cust_flag",
        "c_birth_country",
        "c_login",
    ]

    ws_df = table_reader.read(env, "web_sales", relevant_cols=web_sales_cols)
    ss_df = table_reader.read(env, "store_sales", relevant_cols=store_sales_cols)
#    date_df_1part = table_reader.read(env, "date_dim", relevant_cols=date_cols)
#    customer_df = table_reader.read(env, "customer", relevant_cols=customer_cols)

    return ws_df, ss_df

def main(env: gcy.CylonEnv, config):

    ws_df, ss_df = read_tables(env, config)

    ws_df = ws_df.to_cudf().rename(
        columns={
            "ws_bill_customer_sk": "customer_sk",
        }
    )
    ss_df = ss_df.to_cudf().rename(
        columns={
            "ss_customer_sk": "customer_sk",
        }
    )
    ws_df = gcy.DataFrame.from_cudf(ws_df)
    ss_df = gcy.DataFrame.from_cudf(ss_df)
    print(env.rank, "number of rows in ws_df:", len(ws_df.to_cudf().index))
    print(env.rank, "number of rows in ss_df:", len(ss_df.to_cudf().index))
    print()

    if env.rank == 0:
        print(env.rank, "columns in ws_df:", ws_df.to_cudf().columns)
        print(env.rank, "columns in ss_df:", ss_df.to_cudf().columns)
        print()

    t0 = time.time()
    merged_df = ws_df.merge(ss_df, how="inner", on="customer_sk", env=env)
    total_merge_time = time.time() - t0

    print(env.rank, "number of rows in merged_df:", len(merged_df.to_cudf().index))
    memuse = merged_df.to_cudf().memory_usage(deep=True) / 1000000
#    print("memory usage of each columns in MB for merged_df:")
#    print(memuse)
    print(env.rank, "total size of merged df in MB:", memuse.sum())
    print()

    # print(sales_df.to_arrow())
    # ws_bill_customer_sk: int64
    # first_year_total_web: double
    # second_year_total_web: double
    # ss_customer_sk: int64
    # first_year_total_store: double
    # second_year_total_store: double
    # web_sales_increase_ratio: double

    # sales_df.rename([x.split('-')[1] for x in sales_df.column_names])
    return total_merge_time, env.shuffle_time, env.merge_time


if __name__ == "__main__":

    config = tpcxbb_argparser()
    # print("config:", config)

    mpi_config = gcy.MPIConfig()
    env: gcy.CylonEnv = gcy.CylonEnv(config=mpi_config, distributed=True)

    # set the gpu
    localGpuCount = cupy.cuda.runtime.getDeviceCount()
    deviceID = env.rank % localGpuCount
#    deviceID = (env.rank + 4) % localGpuCount
    deviceID = 0
    cupy.cuda.Device(deviceID).use()
    print("gpu in use:", cupy.cuda.runtime.getDevice(), "by the worker:", env.rank)

    total_merge_time, shuffle_time, merge_time = main(env, config)

    # write the result to results.txt file
    output_file = "single_join_" + str(env.rank) + ".csv"
    f = open(output_file, "w")
    f.write(f"{env.rank},{total_merge_time},{shuffle_time},{merge_time}\n")
    f.close()
    print("rank:", env.rank, total_merge_time, shuffle_time, merge_time)

    env.finalize()

    # first worker should process the output file
    if env.rank == 0:
        import pandas as pd
        import glob

        file_pattern = "single_run_*.csv"
        files = glob.glob(file_pattern)
        df_list = []
        for file in files:
            df1 = pd.read_csv(file, names=["rank", "total_time", "shuffle_time", "merge_time"])
            df_list.append(df1)

        df = pd.concat(df_list)
        df = df.set_index("rank").sort_index()
        if len(df.index) != env.world_size:
            print("number of workers: ", env.world_size)
            print("number of results: ", len(df.index))
            raise ValueError("number of workers is not equal to the number of results!!!!!!")

        tt = "{:.2f}".format(df['total_time'].mean())
        st = "{:.2f}".format(df['shuffle_time'].mean())
        mt = "{:.2f}".format(df['merge_time'].mean())
        f = open("results.csv", "a")
        f.write(f"{tt},{st},{mt}\n")
        f.close()
        print(f"average total_time: {tt}, \
                average shuffle_time: {st}, \
                average merge_time: {mt}")
