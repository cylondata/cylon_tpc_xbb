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

from cylon_xbb_tools.readers import CSVReader
from cylon_xbb_tools.utils import (
    # benchmark,
    tpcxbb_argparser,
    # run_query,
)
import numpy as np


def inventory_before_after(df, date):
    df["inv_before"] = df["inv_quantity_on_hand"].copy()
    df.loc[df["d_date"] >= date, "inv_before"] = 0
    df["inv_after"] = df["inv_quantity_on_hand"].copy()
    df.loc[df["d_date"] < date, "inv_after"] = 0
    return df


def read_tables(ctx, config):
    table_reader = CSVReader(config["data_dir"],
                             rank=None if ctx.get_world_size() == 1 else ctx.get_rank())

    inv_columns = [
        "inv_item_sk",
        "inv_warehouse_sk",
        "inv_date_sk",
        "inv_quantity_on_hand",
    ]

    inventory = table_reader.read(ctx, "inventory", relevant_cols=inv_columns)

    item_columns = ["i_item_id", "i_current_price", "i_item_sk"]
    item = table_reader.read(ctx, "item", relevant_cols=item_columns)

    warehouse_columns = ["w_warehouse_sk", "w_warehouse_name"]
    warehouse = table_reader.read(
        ctx, "warehouse", relevant_cols=warehouse_columns)

    dd_columns = ["d_date_sk", "d_date"]
    date_dim_1part = table_reader.read(ctx, "date_dim", relevant_cols=dd_columns)

    return inventory, item, warehouse, date_dim_1part


def main(ctx, config):
    q22_date = "2001-05-08"
    q22_i_current_price_min = 0.98
    q22_i_current_price_max = 1.5

    inventory, item, warehouse, date_dim_1part = read_tables(ctx, config)

    item = item[(item["i_current_price"] >= q22_i_current_price_min) &
                (item["i_current_price"] <= q22_i_current_price_max)]

    item = item.project(["i_item_id", "i_item_sk"])

    output_table = inventory.join(
        item, left_on=["inv_item_sk"], right_on=["i_item_sk"], join_type='inner', algorithm='sort'
    )

    keep_columns = [
        "inv_warehouse_sk",
        "inv_date_sk",
        "inv_quantity_on_hand",
        "i_item_id",
    ]

    output_table = output_table.project(keep_columns)

    date_dim_1part["d_date"] = date_dim_1part["d_date"].applymap(
        lambda x: x.astype("datetime64[s]").astype("int64") / 86400)

    # Filter limit in days
    min_date = np.datetime64(q22_date, "D").astype(int) - 30
    max_date = np.datetime64(q22_date, "D").astype(int) + 30

    date_dim_1part = date_dim_1part[(
                                date_dim_1part["d_date"] >= min_date) and (
                                    date_dim_1part["d_date"] <= max_date)]

    output_table = output_table.join(
        date_dim_1part, left_on=["inv_date_sk"], right_on=["d_date_sk"], join_type='inner',
        algorithm='sort'
    )

    keep_columns = ["i_item_id", "inv_quantity_on_hand",
                    "inv_warehouse_sk", "d_date"]
    output_table = output_table.project(keep_columns)

    output_table = output_table.join(
        warehouse,
        left_on=["inv_warehouse_sk"],
        right_on=["w_warehouse_sk"],
        join_type='inner', algorithm='sort'
    )

    keep_columns = ["i_item_id", "inv_quantity_on_hand",
                    "d_date", "w_warehouse_name"]
    output_table = output_table.project(keep_columns)

    d_date_int = np.datetime64(q22_date, "D").astype(int)

    output_table["inv_before"] = output_table["inv_quantity_on_hand"]
    output_table["inv_after"] = output_table["inv_quantity_on_hand"]

    # todo below 2 lines are not supported yet
    output_table.loc[output_table["d_date"] >= d_date_int, "inv_before"] = 0
    output_table.loc[output_table["d_date"] < d_date_int, "inv_after"] = 0

    keep_columns = ["i_item_id", "w_warehouse_name", "inv_before", "inv_after"]
    output_table = output_table.project(keep_columns)

    output_table = output_table.groupby(["w_warehouse_name", "i_item_id"], {
        "inv_before": "sum", "inv_after": "sum"
    })

    output_table["inv_ratio"] = output_table["sum_inv_after"] / \
                                output_table["sum_inv_before"]

    ratio_min = 2.0 / 3.0
    ratio_max = 3.0 / 2.0

    output_table = output_table[(
            (output_table["inv_ratio"] >= ratio_min) and (
            output_table["inv_ratio"] <= ratio_max)
    )]

    keep_columns = [
        "w_warehouse_name",
        "i_item_id",
        "sum_inv_before",
        "sum_inv_after",
    ]
    output_table = output_table[keep_columns]

    # for query 22 the results vary after 6 th decimal place
    return output_table[0:100]


if __name__ == "__main__":
    config = tpcxbb_argparser()

    from pycylon import CylonContext
    from pycylon.net import MPIConfig

    mpi_config = MPIConfig()
    ctx: CylonContext = CylonContext(config=mpi_config, distributed=True)

    print(main(ctx, config))
