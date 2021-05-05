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


def read_tables(env: CylonEnv, config) -> Iterable[DataFrame]:
    table_reader = CSVReader(config["data_dir"], rank=None if env.world_size == 1 else env.rank)

    ddim_columns = ["d_date_sk", "d_year", "d_moy"]

    date_dim_table_1part = table_reader.read(
        env, "date_dim", relevant_cols=ddim_columns)

    inv_columns = [
        "inv_warehouse_sk",
        "inv_item_sk",
        "inv_date_sk",
        "inv_quantity_on_hand",
    ]

    inventory_table = table_reader.read(env, "inventory", relevant_cols=inv_columns)

    return date_dim_table_1part, inventory_table


def main(env: CylonEnv, config) -> DataFrame:
    q23_year = 2001
    q23_month = 1
    q23_coefficient = 1.3

    date_dim_table_1part, inventory_table = read_tables(env, config)

    # Query Set 1

    inventory_data_dim_joined = inventory_table.merge(date_dim_table_1part, how='inner',
                                                      algorithm='sort', left_on=['inv_date_sk'],
                                                      right_on=['d_date_sk'], suffixes=('', ''))

    q23_month_plus_one = q23_month + 1
    inv_dates_result = inventory_data_dim_joined[
        (inventory_data_dim_joined['d_year'] == q23_year) & (
                inventory_data_dim_joined['d_moy'] >= q23_month) & (
                inventory_data_dim_joined['d_moy'] <= q23_month_plus_one)]

    print("Printing join results")
    print(inv_dates_result[0:3])

    # Query Set 2

    grouped_inv_dates = inv_dates_result.groupby(by=["inv_warehouse_sk", "inv_item_sk", "d_moy"],
                                                 env=env) \
        .agg({"inv_quantity_on_hand": ["mean", "std"]})

    # todo remove this indeixng call
    index_arr = [i for i in range(0, len(grouped_inv_dates))]
    grouped_inv_dates.set_index(index_arr)

    grouped_inv_dates.rename({
        "mean_inv_quantity_on_hand": "qty_mean",
        "std_inv_quantity_on_hand": "qty_std",
    })

    print("Aggregated table")
    print(grouped_inv_dates)

    # Query Set 3

    grouped_inv_dates["qty_cov"] = (
            grouped_inv_dates["qty_std"] / grouped_inv_dates["qty_mean"]
    )

    grouped_inv_dates_where = grouped_inv_dates[(
            grouped_inv_dates["qty_cov"] > q23_coefficient)]

    selected_columns = grouped_inv_dates_where[[
        "inv_warehouse_sk", "inv_item_sk", "d_moy", "qty_cov"]]

    # Final filtering

    inv1_df = selected_columns[(selected_columns["d_moy"] == q23_month)]

    inv2_df = selected_columns[(selected_columns["d_moy"] == q23_month + 1)]

    result_df = inv1_df.merge(inv2_df, how='inner', algorithm='sort',
                              left_on=['inv_warehouse_sk', 'inv_item_sk'],
                              right_on=['inv_warehouse_sk', 'inv_item_sk'],
                              suffixes=('l_', 'r_'), env=env)

    print("Before rename")
    print(result_df[0:10])

    # todo remove this indeixng call
    index_arr = [i for i in range(0, len(result_df))]
    result_df.set_index(index_arr)
    result_df.rename({
        "l_d_moy": "d_moy",
        "r_d_moy": "inv2_d_moy",
        "l_qty_cov": "cov",
        "r_qty_cov": "inv2_cov",
    })

    result_df = result_df.sort_values(by=["l_inv_warehouse_sk", "l_inv_item_sk"], env=env)

    # printing first 10 rows
    print(result_df[0:10])

    return result_df


if __name__ == "__main__":
    config = tpcxbb_argparser()

    mpi_config = MPIConfig()
    ctx: CylonEnv = CylonEnv(config=mpi_config, distributed=True)

    res = main(ctx, config)

    if ctx.rank == 0:
        import os

        os.makedirs(config['output_dir'], exist_ok=True)
        res.to_pandas().to_csv(f"{config['output_dir']}/q23_results.csv", index=False)

    ctx.finalize()
