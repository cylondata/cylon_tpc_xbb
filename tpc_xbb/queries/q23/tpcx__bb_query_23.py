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

from tpc_xbb.tools.readers import CSVReader
from tpc_xbb.tools.utils import (
    # benchmark,
    tpcxbb_argparser,
    # run_query,
)


def read_tables(ctx, config):
    table_reader = CSVReader(config["data_dir"], rank=None)

    ddim_columns = ["d_date_sk", "d_year", "d_moy"]

    date_dim_table = table_reader.read(
        ctx, "date_dim", relevant_cols=ddim_columns)

    inv_columns = [
        "inv_warehouse_sk",
        "inv_item_sk",
        "inv_date_sk",
        "inv_quantity_on_hand",
    ]

    inventory_table = table_reader.read(
        ctx, "inventory", relevant_cols=inv_columns)

    return date_dim_table, inventory_table


def main(ctx, config):
    q23_year = 2001
    q23_month = 1
    q23_coefficient = 1.3

    date_dim_table, inventory_table = read_tables(ctx, config)

    # Query Set 1

    inventory_data_dim_joined = inventory_table.distributed_join(
        table=date_dim_table, join_type='inner', algorithm='sort', left_on=['inv_date_sk'], right_on=['d_date_sk'])

    q23_month_plus_one = q23_month+1
    inv_dates_result = inventory_data_dim_joined[(inventory_data_dim_joined['d_year'] == q23_year) & (
        inventory_data_dim_joined['d_moy'] >= q23_month) & (inventory_data_dim_joined['d_moy'] <= q23_month_plus_one)]

    print("Printing join results")
    print(inv_dates_result[0:3])

    # Query Set 2

    grouped_inv_dates = inv_dates_result.groupby(["inv_warehouse_sk", "inv_item_sk", "d_moy"], {
        "inv_quantity_on_hand": ["mean", "std"]
    })

    # todo remove this indeixng call
    index_arr = [i for i in range(0, grouped_inv_dates.row_count)]
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

    inv2_df = selected_columns[(selected_columns["d_moy"] == q23_month+1)]

    result_df = inv1_df.join(table=inv2_df,
                             join_type='inner', algorithm='sort', left_on=['inv_warehouse_sk', 'inv_item_sk'], right_on=['inv_warehouse_sk', 'inv_item_sk'],
                             left_prefix='l_', right_prefix='r_')

    # todo remove this indeixng call
    print("Before rename")
    print(result_df[0:10])

    index_arr = [i for i in range(0, result_df.row_count)]
    result_df.set_index(index_arr)
    result_df.rename({
        "l_d_moy": "d_moy",
        "r_d_moy": "inv2_d_moy",
        "l_qty_cov": "cov",
        "r_qty_cov": "inv2_cov",
    })

    result_df = result_df.sort(["l_inv_warehouse_sk", "l_inv_item_sk"])

    # printing first 10 rows
    print(result_df[0:10])


if __name__ == "__main__":

    config = tpcxbb_argparser()

    from pycylon import CylonContext
    from pycylon.net import MPIConfig

    mpi_config = MPIConfig()
    ctx: CylonContext = CylonContext(config=mpi_config, distributed=True)

    main(ctx, config)
