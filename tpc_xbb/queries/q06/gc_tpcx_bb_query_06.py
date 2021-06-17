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

import pygcylon as gc

q06_YEAR = 2001
q6_limit_rows = 100


def read_tables(env: gc.CylonEnv, config) -> Iterable[gc.DataFrame]:
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
    date_df_1part = table_reader.read(env, "date_dim", relevant_cols=date_cols)
    customer_df = table_reader.read(env, "customer", relevant_cols=customer_cols)

    return ws_df, ss_df, date_df_1part, customer_df


def get_sales_ratio(env: gc.CylonEnv, df: gc.DataFrame, table="store_sales") -> gc.DataFrame:
    assert table in ("store_sales", "web_sales")

    if table == "store_sales":
        column_prefix = "ss_"
    else:
        column_prefix = "ws_"

    f_year = q06_YEAR
    s_year = q06_YEAR + 1

    first_year_flag = (df.to_cudf()["d_year"] == f_year)
    second_year_flag = (df.to_cudf()["d_year"] == s_year)

    df_f_year = df[first_year_flag]
    df_f_year["first_year_sales"] = (df_f_year[f"{column_prefix}ext_list_price"]
                                     - df_f_year[f"{column_prefix}ext_wholesale_cost"]
                                     - df_f_year[f"{column_prefix}ext_discount_amt"]
                                     + df_f_year[f"{column_prefix}ext_sales_price"]) / 2
    df_f_year["second_year_sales"] = 0.0

    df_s_year = df[second_year_flag]
    df_s_year['first_year_sales'] = 0.0
    df_s_year["second_year_sales"] = (df_s_year[f"{column_prefix}ext_list_price"]
                                      - df_s_year[f"{column_prefix}ext_wholesale_cost"]
                                      - df_s_year[f"{column_prefix}ext_discount_amt"]
                                      + df_s_year[f"{column_prefix}ext_sales_price"]) / 2

    return gc.concat([df_f_year, df_s_year], axis=0)


def main(env: gc.CylonEnv, config):

    ws_df, ss_df, date_df_1part, customer_df = read_tables(env, config)
    t0 = time.time()

    """
    filtered_date_df = date_df.query(
        f"d_year >= {q06_YEAR} and d_year <= {q06_YEAR+1}", meta=date_df._meta
    ).reset_index(drop=True)"""
    filtered_date_df_1part = date_df_1part[
        (date_df_1part['d_year'] >= q06_YEAR) & (date_df_1part['d_year'] <= (q06_YEAR + 1))]

    """
    web_sales_df = ws_df.merge(
        filtered_date_df, left_on="ws_sold_date_sk", right_on="d_date_sk", how="inner"
    )"""
    web_sales_df = ws_df.merge(filtered_date_df_1part, how="inner", algorithm='hash',
                               left_on=["ws_sold_date_sk"], right_on=["d_date_sk"],
                               suffixes=('', ''))  # local
    # print(web_sales_df.to_arrow())
    # ws_bill_customer_sk: int64
    # ws_sold_date_sk: int64
    # ws_ext_list_price: double
    # ws_ext_wholesale_cost: double
    # ws_ext_discount_amt: double
    # ws_ext_sales_price: double
    # d_date_sk: int64
    # d_year: int64
    # d_moy: int64

    """
    ws_grouped_df = (
        web_sales_df.groupby(by=["ws_bill_customer_sk", "d_year"])
            .agg(
            {
                "ws_ext_list_price": "sum",
                "ws_ext_wholesale_cost": "sum",
                "ws_ext_discount_amt": "sum",
                "ws_ext_sales_price": "sum",
            }
        )
            .reset_index()
    )"""
    ws_grouped_df = web_sales_df.groupby(by=["ws_bill_customer_sk", "d_year"], env=env) \
        .agg({"ws_ext_list_price": "sum",
              "ws_ext_wholesale_cost": "sum",
              "ws_ext_discount_amt": "sum",
              "ws_ext_sales_price": "sum"}
             ).reset_index()
    #     print(ws_grouped_df.to_arrow())
    # ws_bill_customer_sk: int64
    # d_year: int64
    # sum_ws_ext_list_price: double
    # sum_ws_ext_wholesale_cost: double
    # sum_ws_ext_discount_amt: double
    # sum_ws_ext_sales_price: double

    # ws_grouped_df._cdf.rename([x.replace('sum_', '') for x in ws_grouped_df._cdf.columns], inplace=True)
    #     print(ws_grouped_df.to_arrow())
    # ws_bill_customer_sk: int64
    # d_year: int64
    # ws_ext_list_price: double
    # ws_ext_wholesale_cost: double
    # ws_ext_discount_amt: double
    # ws_ext_sales_price: double

    """
    web_sales_ratio_df = ws_grouped_df.map_partitions(
        get_sales_ratio, table="web_sales"
    )"""
    web_sales_ratio_df = get_sales_ratio(env, ws_grouped_df, table="web_sales")
    #     print(web_sales_ratio_df, web_sales_ratio_df.row_count)

    """
    web_sales = (
        web_sales_ratio_df.groupby(["ws_bill_customer_sk"])
        .agg({"first_year_sales": "sum", "second_year_sales": "sum"})
        .reset_index()
    )
        web_sales = web_sales.loc[web_sales["first_year_sales"] > 0].reset_index(drop=True)
    """
    web_sales = web_sales_ratio_df.groupby(by=["ws_bill_customer_sk"], env=env) \
        .agg({"first_year_sales": "sum",
              "second_year_sales": "sum"})\
        .reset_index()

    web_sales = web_sales._cdf.loc[web_sales["first_year_sales"] > 0].reset_index(drop=True)
    """
    web_sales = web_sales.rename(
        columns={
            "first_year_sales": "first_year_total_web",
            "second_year_sales": "second_year_total_web",
        }
    )
    """
    #     print(web_sales, web_sales.row_count)
    web_sales = web_sales.rename(
        columns={
            "first_year_sales": "first_year_total_web",
            "second_year_sales": "second_year_total_web",
        }
    )
    web_sales = gc.DataFrame.from_cudf(web_sales)
    # ws_bill_customer_sk: int64
    # first_year_total_web: double
    # second_year_total_web: double
    """
    store_sales_df = ss_df.merge(
        filtered_date_df, left_on="ss_sold_date_sk", right_on="d_date_sk", how="inner"
    )"""
    store_sales_df = ss_df.merge(filtered_date_df_1part, how="inner", algorithm='hash',
                                 left_on="ss_sold_date_sk", right_on="d_date_sk")  # local
    #     print(store_sales_df.to_arrow())
    # store_sales_df.rename([x.split('-')[1] for x in store_sales_df.column_names])
    # ss_customer_sk: int64
    # ss_sold_date_sk: int64
    # ss_ext_list_price: double
    # ss_ext_wholesale_cost: double
    # ss_ext_discount_amt: double
    # ss_ext_sales_price: double
    # d_date_sk: int64
    # d_year: int64
    # d_moy: int64

    """
    ss_grouped_df = (
        store_sales_df.groupby(by=["ss_customer_sk", "d_year"])
            .agg(
            {
                "ss_ext_list_price": "sum",
                "ss_ext_wholesale_cost": "sum",
                "ss_ext_discount_amt": "sum",
                "ss_ext_sales_price": "sum",
            }
        )
            .reset_index()
    )
    """
    ss_grouped_df = store_sales_df.groupby(by=["ss_customer_sk", "d_year"], env=env)\
        .agg({"ss_ext_list_price": "sum",
              "ss_ext_wholesale_cost": "sum",
              "ss_ext_discount_amt": "sum",
              "ss_ext_sales_price": "sum"})\
        .reset_index()

    """
    store_sales_ratio_df = ss_grouped_df.map_partitions(
        get_sales_ratio, table="store_sales"
    )
    """
    store_sales_ratio_df = get_sales_ratio(env, ss_grouped_df, table="store_sales")

    #     print(store_sales_ratio_df, store_sales_ratio_df.row_count)

    # ss_customer_sk: int64
    # d_year: int64
    # ss_ext_list_price: double
    # ss_ext_wholesale_cost: double
    # ss_ext_discount_amt: double
    # ss_ext_sales_price: double
    # first_year_sales: double
    # second_year_sales: double

    """
    store_sales = (
        store_sales_ratio_df.groupby(["ss_customer_sk"])
            .agg({"first_year_sales": "sum", "second_year_sales": "sum"})
            .reset_index()
    )
    store_sales = store_sales.loc[store_sales["first_year_sales"] > 0].reset_index(
        drop=True
    )
    store_sales = store_sales.rename(
        columns={
            "first_year_sales": "first_year_total_store",
            "second_year_sales": "second_year_total_store",
        }
    )"""
    store_sales = store_sales_ratio_df.groupby(by=["ss_customer_sk"], env=env) \
        .agg({"first_year_sales": "sum", "second_year_sales": "sum"})\
        .reset_index()
    store_sales = store_sales._cdf.loc[store_sales["first_year_sales"] > 0].reset_index(
        drop=True
    )
    store_sales = store_sales.rename(
        columns={
            "first_year_sales": "first_year_total_store",
            "second_year_sales": "second_year_total_store",
        }
    )
    store_sales = gc.DataFrame.from_cudf(store_sales)
    # ss_customer_sk: int64
    # first_year_total_store: double
    # second_year_total_store: double

    # SQL "AS"
    """    
    sales_df = web_sales.merge(
        store_sales,
        left_on="ws_bill_customer_sk",
        right_on="ss_customer_sk",
        how="inner",
    )
    sales_df["web_sales_increase_ratio"] = (
            sales_df["second_year_total_web"] / sales_df["first_year_total_web"]
    )
    """
    # print(store_sales)
    sales_df = web_sales.merge(store_sales,
                               how="inner",
                               left_on=["ws_bill_customer_sk"],
                               right_on=["ss_customer_sk"],
                               suffixes=('', ''),
                               env=env)

    # sales_df.rename([x.split('-')[1] for x in sales_df.column_names])
    # print(sales_df.to_arrow())
    sales_df["web_sales_increase_ratio"] = sales_df["second_year_total_web"] \
                                           / sales_df["first_year_total_web"]
    # print(sales_df.to_arrow())
    # ws_bill_customer_sk: int64
    # first_year_total_web: double
    # second_year_total_web: double
    # ss_customer_sk: int64
    # first_year_total_store: double
    # second_year_total_store: double
    # web_sales_increase_ratio: double

    # Join the customer with the combined web and store sales.
    """ 
    customer_df["c_customer_sk"] = customer_df["c_customer_sk"].astype("int64")
    sales_df["ws_bill_customer_sk"] = sales_df["ws_bill_customer_sk"].astype("int64")
    sales_df = sales_df.merge(
        customer_df,
        left_on="ws_bill_customer_sk",
        right_on="c_customer_sk",
        how="inner",
    ).reset_index(drop=True)"""
    customer_df["c_customer_sk"] = customer_df["c_customer_sk"].astype("int64")
    sales_df["ws_bill_customer_sk"] = sales_df["ws_bill_customer_sk"].astype("int64")
    sales_df = sales_df.merge(customer_df,
                              how="inner",
                              left_on=["ws_bill_customer_sk"],
                              right_on=["c_customer_sk"],
                              suffixes=('', ''),
                              env=env)\
        .reset_index(drop=True)
    # sales_df.rename([x.split('-')[1] for x in sales_df.column_names])
    elapsed_time = time.time() - t0
#    print("columns of sales_df:", sales_df.to_cudf().columns)
    print("number of rows of sales_df:", len(sales_df.to_cudf().index))
    return elapsed_time


if __name__ == "__main__":

    config = tpcxbb_argparser()
    # print("config:", config)

    mpi_config = gc.MPIConfig()
    env: gc.CylonEnv = gc.CylonEnv(config=mpi_config, distributed=True)

    # set the gpu
    localGpuCount = cupy.cuda.runtime.getDeviceCount()
    deviceID = env.rank % localGpuCount
    cupy.cuda.Device(deviceID).use()
    print("gpu in use:", cupy.cuda.runtime.getDevice(), "by the worker:", env.rank)

    elapsed_time = main(env, config)

    time.sleep(env.rank * 0.1)

    # write the result to results.txt file
    f = open("single_run_results.csv", "a")
    f.write(f"{env.rank},{elapsed_time}\n")
    f.close()
    print("rank:", env.rank, "elapsed_time:", elapsed_time)

    time.sleep(3)

    env.finalize()
