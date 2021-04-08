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

from cylon_xbb_tools.utils import (
    # benchmark,
    tpcxbb_argparser,
    # run_query,
)
from cylon_xbb_tools.readers import CSVReader

from pycylon import Table
from pycylon.data.aggregates import AggregationOp

q07_HIGHER_PRICE_RATIO = 1.2
# --store_sales date
q07_YEAR = 2004
q07_MONTH = 7
q07_HAVING_COUNT_GE = 10
q07_LIMIT = 10


def create_high_price_items_df(ctx, item_df):
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
    grouped_item_df = item_df[["i_category", "i_current_price"]]. \
        groupby(index=0, agg={"i_current_price": 'mean'})
    # [i_category, i_current_price_mean]

    grouped_item_df.rename({"mean_i_current_price": "avg_price"})
    # [i_category, avg_price]

    item_df = item_df.distributed_join(grouped_item_df, join_type='inner', algorithm='sort',
                                       on=['i_category']).drop(['rt-i_category'])
    # [lt-i_item_sk, lt-i_current_price, lt-i_category, rt-avg_price]

    item_df = item_df[
        item_df["lt-i_current_price"] > item_df["rt-avg_price"] * q07_HIGHER_PRICE_RATIO]
    # [lt-i_item_sk, lt-i_current_price, lt-i_category, rt-avg_price]

    # item_df.rename([x.split('-')[1] for x in item_df.column_names])

    return item_df


def read_tables(ctx, config):
    # table_reader = build_reader(
    #     data_format=config["file_format"],
    #     basepath=config["data_dir"],
    #     split_row_groups=config["split_row_groups"],
    # )
    # table_reader = CSVReader(config["data_dir"], ctx.get_rank())
    table_reader = CSVReader(config["data_dir"],
                             rank=None if ctx.get_world_size() == 1 else ctx.get_rank())

    item_cols = ["i_item_sk", "i_current_price", "i_category"]
    store_sales_cols = ["ss_item_sk", "ss_customer_sk", "ss_sold_date_sk"]
    store_cols = ["s_store_sk"]
    date_cols = ["d_date_sk", "d_year", "d_moy"]
    customer_cols = ["c_customer_sk", "c_current_addr_sk"]
    customer_address_cols = ["ca_address_sk", "ca_state"]

    item_df = table_reader.read(ctx, "item", relevant_cols=item_cols)
    store_sales_df = table_reader.read(ctx, "store_sales", relevant_cols=store_sales_cols)
    store_df = table_reader.read(ctx, "store", relevant_cols=store_cols)
    date_dim_df_1part = table_reader.read(ctx, "date_dim", relevant_cols=date_cols)
    customer_df = table_reader.read(ctx, "customer", relevant_cols=customer_cols)
    customer_address_df = table_reader.read(ctx, "customer_address",
                                            relevant_cols=customer_address_cols)

    return (
        item_df,
        store_sales_df,
        store_df,
        date_dim_df_1part,
        customer_df,
        customer_address_df,
    )


def main(ctx, config):
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
    ) = read_tables(ctx, config)

    # i_item_sk  i_current_price                i_category  avg_price
    high_price_items_df = create_high_price_items_df(ctx, item_df)
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
    store_sales_df = store_sales_df.join(
        filtered_date_df_1part, join_type='inner', algorithm='sort',
        left_on=["ss_sold_date_sk"],
        right_on=["d_date_sk"], )
    # lt-ss_item_sk  lt-ss_customer_sk  lt-ss_sold_date_sk  rt-d_date_sk  rt-d_year  rt-d_moy

    # cols 2 keep after merge
    """
    store_sales_cols = ["ss_item_sk", "ss_customer_sk", "ss_sold_date_sk"]
    store_sales_df = store_sales_df[store_sales_cols]
    """
    store_sales_cols = ["lt-ss_item_sk", "lt-ss_customer_sk", "lt-ss_sold_date_sk"]
    store_sales_df = store_sales_df[store_sales_cols]
    # store_sales_df.rename([x.split('-')[1] for x in store_sales_cols])
    # ["ss_item_sk", "ss_customer_sk", "ss_sold_date_sk"]

    # Query 1. `store_sales` join `highPriceItems`1 q
    """    
    store_sales_high_price_items_join_df = store_sales_df.merge(
        high_price_items_df, left_on=["ss_item_sk"], right_on=["i_item_sk"], how="inner"
    )
    """
    store_sales_high_price_items_join_df = store_sales_df.distributed_join(
        high_price_items_df, join_type='inner', algorithm='sort', left_on=["ss_item_sk"],
        right_on=["i_item_sk"],
    )
    # store_sales_high_price_items_join_df.rename(
    #     [x.split('-')[1] for x in store_sales_high_price_items_join_df.column_names])

    # Query 2. `Customer` Merge `store_sales_highPriceItems_join_df`
    """
    store_sales_high_price_items_customer_join_df = store_sales_high_price_items_join_df.merge(
        customer_df, left_on=["ss_customer_sk"], right_on=["c_customer_sk"], how="inner"
    )
    """
    store_sales_high_price_items_customer_join_df = store_sales_high_price_items_join_df \
        .distributed_join(customer_df, join_type='inner', algorithm='sort',
                          left_on=["ss_customer_sk"], right_on=["c_customer_sk"], )
    # store_sales_high_price_items_customer_join_df.rename(
    #     [x.split('-')[1] for x in store_sales_high_price_items_customer_join_df.column_names])

    # Query 3. `store_sales_highPriceItems_customer_join_df` Merge `Customer Address`
    customer_address_df = customer_address_df[customer_address_df["ca_state"].notnull()]

    """
    final_merged_df = store_sales_high_price_items_customer_join_df.merge(
        customer_address_df, left_on=["c_current_addr_sk"], right_on=["ca_address_sk"]
    )
    """
    final_merged_df = store_sales_high_price_items_customer_join_df.distributed_join(
        customer_address_df, join_type='inner', algorithm='sort', left_on=["c_current_addr_sk"],
        right_on=["ca_address_sk"])
    # final_merged_df.rename([x.split('-')[1] for x in final_merged_df.column_names])

    # Query 4. Final State Grouped Query
    """
    count_df = final_merged_df["ca_state"].value_counts()
    """
    # todo this is inefficient. replace with a value count op
    count_df = final_merged_df[["ca_state", "ca_address_sk"]] \
        .groupby(index=0, agg={1: AggregationOp.COUNT})
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

    sorted_df = count_df.sort(1, ascending=False)

    result_df = sorted_df[0:q07_LIMIT]
    print(result_df)

    return result_df


if __name__ == "__main__":
    # from cylon_xbb_tools.cluster_startup import attach_to_cluster
    # import cudf
    # import dask_cudf

    config = tpcxbb_argparser()
    # client, bc = attach_to_cluster(config)
    # run_query(config=config, client=client, query_func=main)

    from pycylon import CylonContext
    from pycylon.net import MPIConfig

    mpi_config = MPIConfig()
    ctx: CylonContext = CylonContext(config=mpi_config, distributed=True)

    main(ctx, config)
