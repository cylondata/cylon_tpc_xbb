##
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##
from typing import Iterable

from cylon_xbb_tools.utils import (
    # benchmark,
    tpcxbb_argparser,
    # run_query,
)
from cylon_xbb_tools.readers import CSVReader

from pycylon.net import MPIConfig
from pycylon import CylonEnv, DataFrame

q07_HIGHER_PRICE_RATIO = 1.2
# --store_sales date
q07_YEAR = 2004
q07_MONTH = 7
q07_HAVING_COUNT_GE = 10
q07_LIMIT = 10


def create_high_price_items_df(env: CylonEnv, item_df: DataFrame) -> DataFrame:
    """
    grouped_item_df = (
        item_df[["i_category", "i_current_price"]]
            .groupby(["i_category"])
            .agg({"i_current_price": "mean"})
    )
    grouped_item_df = grouped_item_df.rename(columns={"i_current_price": "avg_price"})
    grouped_item_df = grouped_item_df.reset_index(drop=False)

    item_df = item_df.merge(grouped_item_df)
    item_df = item_df[
        item_df["i_current_price"] > item_df["avg_price"] * q07_HIGHER_PRICE_RATIO
        ].reset_index(drop=True)
    high_price_items_df = item_df
    del item_df
    return high_price_items_df
    """
    # item_df = ["i_item_sk", "i_current_price", "i_category"]
    grouped_item_df = item_df[["i_category", "i_current_price"]].groupby(by=0, env=env) \
        .agg({"i_current_price": 'mean'})
    # [i_category, i_current_price_mean]

    grouped_item_df.rename({"mean_i_current_price": "avg_price"})
    # [i_category, avg_price]

    # todo: this join has unbalanced work & can create empty tables
    item_df = item_df.merge(grouped_item_df, how='inner', algorithm='sort',
                            on=['i_category'], suffixes=("", "rt-"), env=env) \
        .drop(['rt-i_category'])
    item_df.rename({"rt-avg_price": "avg_price"})
    # [i_item_sk, i_current_price, i_category, avg_price]

    if len(item_df):
        item_df = item_df[
            item_df["i_current_price"] > item_df["avg_price"] * q07_HIGHER_PRICE_RATIO]
        # [i_item_sk, i_current_price, i_category, avg_price]

    return item_df


def read_tables(env: CylonEnv, config):
    # table_reader = build_reader(
    #     data_format=config["file_format"],
    #     basepath=config["data_dir"],
    #     split_row_groups=config["split_row_groups"],
    # )
    # table_reader = CSVReader(config["data_dir"], ctx.get_rank())
    table_reader = CSVReader(config["data_dir"], rank=None if env.world_size == 1 else env.rank)

    item_cols = ["i_item_sk", "i_current_price", "i_category"]
    store_sales_cols = ["ss_item_sk", "ss_customer_sk", "ss_sold_date_sk"]
    store_cols = ["s_store_sk"]
    date_cols = ["d_date_sk", "d_year", "d_moy"]
    customer_cols = ["c_customer_sk", "c_current_addr_sk"]
    customer_address_cols = ["ca_address_sk", "ca_state"]

    item_df = table_reader.read(env, "item", relevant_cols=item_cols)
    store_sales_df = table_reader.read(env, "store_sales", relevant_cols=store_sales_cols)
    store_df = table_reader.read(env, "store", relevant_cols=store_cols)
    date_dim_df_1part = table_reader.read(env, "date_dim", relevant_cols=date_cols)
    customer_df = table_reader.read(env, "customer", relevant_cols=customer_cols)
    customer_address_df = table_reader.read(env, "customer_address",
                                            relevant_cols=customer_address_cols)

    return (
        item_df,
        store_sales_df,
        store_df,
        date_dim_df_1part,
        customer_df,
        customer_address_df,
    )


def main(env: CylonEnv, config):
    # (
    #     item_df,
    #     store_sales_df,
    #     store_df,
    #     date_dim_df,
    #     customer_df,
    #     customer_address_df,
    # ) = benchmark(
    #     read_tables,
    #     config=config,
    #     compute_result=config["get_read_time"],
    #     dask_profile=config["dask_profile"],
    # )

    (
        item_df,
        store_sales_df,
        store_df,
        date_dim_df_1part,
        customer_df,
        customer_address_df,
    ) = read_tables(env, config)

    # i_item_sk  i_current_price                i_category  avg_price
    high_price_items_df = create_high_price_items_df(env, item_df)
    del item_df

    # Query 0. Date Time Filteration Logic
    # d_date_sk  d_year  d_moy
    """
    filtered_date_df = date_dim_df.query(
        f"d_year == {q07_YEAR} and d_moy == {q07_MONTH}", meta=date_dim_df._meta
    ).reset_index(drop=True)
    """
    filtered_date_df_1part = date_dim_df_1part[
        (date_dim_df_1part['d_year'] == q07_YEAR) & (date_dim_df_1part['d_moy'] == q07_MONTH)]

    # filtering store sales to above dates
    # ss_item_sk  ss_customer_sk  ss_sold_date_sk
    """
    store_sales_df = store_sales_df.merge(
        filtered_date_df,
        left_on=["ss_sold_date_sk"],
        right_on=["d_date_sk"],
        how="inner",
    )
    """
    # local merge
    store_sales_df = store_sales_df.merge(filtered_date_df_1part, how='inner', algorithm='sort',
                                          left_on=["ss_sold_date_sk"], right_on=["d_date_sk"],
                                          suffixes=("", ""))
    # ss_item_sk  ss_customer_sk  ss_sold_date_sk  d_date_sk  d_year  d_moy

    # cols 2 keep after merge
    """
    store_sales_cols = ["ss_item_sk", "ss_customer_sk", "ss_sold_date_sk"]
    store_sales_df = store_sales_df[store_sales_cols]
    """
    store_sales_cols = ["ss_item_sk", "ss_customer_sk", "ss_sold_date_sk"]
    store_sales_df = store_sales_df[store_sales_cols]
    # ["ss_item_sk", "ss_customer_sk", "ss_sold_date_sk"]

    # Query 1. `store_sales` join `highPriceItems`1 q
    """    
    store_sales_high_price_items_join_df = store_sales_df.merge(
        high_price_items_df, left_on=["ss_item_sk"], right_on=["i_item_sk"], how="inner"
    )
    """
    store_sales_high_price_items_join_df = store_sales_df.merge(high_price_items_df,
                                                                how='inner', algorithm='sort',
                                                                left_on=["ss_item_sk"],
                                                                right_on=["i_item_sk"],
                                                                suffixes=("", ""),
                                                                env=env)

    # Query 2. `Customer` Merge `store_sales_highPriceItems_join_df`
    """
    store_sales_high_price_items_customer_join_df = store_sales_high_price_items_join_df.merge(
        customer_df, left_on=["ss_customer_sk"], right_on=["c_customer_sk"], how="inner"
    )
    """
    store_sales_high_price_items_customer_join_df = store_sales_high_price_items_join_df \
        .merge(customer_df, how='inner', algorithm='sort', left_on=["ss_customer_sk"],
               right_on=["c_customer_sk"], suffixes=("", ""), env=env)

    # Query 3. `store_sales_highPriceItems_customer_join_df` Merge `Customer Address`
    customer_address_df = customer_address_df[customer_address_df["ca_state"].notnull()]

    """
    final_merged_df = store_sales_high_price_items_customer_join_df.merge(
        customer_address_df, left_on=["c_current_addr_sk"], right_on=["ca_address_sk"]
    )
    """
    final_merged_df = store_sales_high_price_items_customer_join_df.merge(
        customer_address_df, how='inner', algorithm='sort', left_on=["c_current_addr_sk"],
        right_on=["ca_address_sk"], suffixes=("", ""), env=env)

    # Query 4. Final State Grouped Query
    """
    count_df = final_merged_df["ca_state"].value_counts()
    """
    # todo this is inefficient. replace with a value count op
    count_df = final_merged_df[["ca_state", "ca_address_sk"]].groupby(by=0, env=env).agg({1: "cnt"})
    count_df.rename(["ca_state", "cnt"])

    # number of states is limited=50
    # so it can remain a cudf frame
    """
    count_df = count_df.compute()
    count_df = count_df[count_df >= q07_HAVING_COUNT_GE]
    count_df = count_df.sort_values(ascending=False)

    result_df = count_df.head(q07_LIMIT)
    result_df = result_df.reset_index(drop=False).rename(
        columns={"index": "ca_state", "ca_state": "cnt"}
    )
    """
    count_df = count_df[count_df["cnt"] >= q07_HAVING_COUNT_GE]

    sorted_df = count_df.sort_values(1, ascending=False, env=env)

    result_df = sorted_df[0:q07_LIMIT]

    return result_df


if __name__ == "__main__":
    config = tpcxbb_argparser()

    mpi_config = MPIConfig()
    ctx: CylonEnv = CylonEnv(config=mpi_config, distributed=True)

    res = main(ctx, config)

    if ctx.rank == 0:
        import os

        os.makedirs(config['output_dir'], exist_ok=True)
        res.to_pandas().to_csv(f"{config['output_dir']}/q07_results.csv", index=False)

    ctx.finalize()
