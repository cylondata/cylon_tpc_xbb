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

from cylon_xbb_tools.readers import CSVReader
from cylon_xbb_tools.utils import (
    # benchmark,
    tpcxbb_argparser,
    # run_query,
)
import numpy as np

from pycylon.net import MPIConfig
from pycylon import CylonEnv, DataFrame


def inventory_before_after(df: DataFrame, date) -> DataFrame:
    df["inv_before"] = df["inv_quantity_on_hand"].copy()
    df.loc[df["d_date"] >= date, "inv_before"] = 0
    df["inv_after"] = df["inv_quantity_on_hand"].copy()
    df.loc[df["d_date"] < date, "inv_after"] = 0
    return df


def read_tables(env: CylonEnv, config) -> Iterable[DataFrame]:
    table_reader = CSVReader(config["data_dir"], rank=None if env.world_size == 1 else env.rank)

    inv_columns = [
        "inv_item_sk",
        "inv_warehouse_sk",
        "inv_date_sk",
        "inv_quantity_on_hand",
    ]

    inventory = table_reader.read(env, "inventory", relevant_cols=inv_columns)

    item_columns = ["i_item_id", "i_current_price", "i_item_sk"]
    item = table_reader.read(env, "item", relevant_cols=item_columns)

    warehouse_columns = ["w_warehouse_sk", "w_warehouse_name"]
    warehouse = table_reader.read(
        env, "warehouse", relevant_cols=warehouse_columns)

    dd_columns = ["d_date_sk", "d_date"]
    date_dim_1part = table_reader.read(env, "date_dim", relevant_cols=dd_columns)

    return inventory, item, warehouse, date_dim_1part


def main(env, config):
    q22_date = "2001-05-08"
    q22_i_current_price_min = 0.98
    q22_i_current_price_max = 1.5

    inventory, item, warehouse, date_dim_1part = read_tables(env, config)

    item = item[(item["i_current_price"] >= q22_i_current_price_min) &
                (item["i_current_price"] <= q22_i_current_price_max)]

    item = item[["i_item_id", "i_item_sk"]]

    output_table = inventory.merge(item, left_on=["inv_item_sk"], right_on=["i_item_sk"],
                                   how='inner', algorithm='sort', suffixes=('', ''), env=env)

    keep_columns = ["inv_warehouse_sk",
                    "inv_date_sk",
                    "inv_quantity_on_hand",
                    "i_item_id", ]

    output_table = output_table[keep_columns]

    date_dim_1part["d_date"] = date_dim_1part["d_date"].applymap(
        lambda x: x.astype("datetime64[s]").astype("int64") / 86400)

    # Filter limit in days
    min_date = np.datetime64(q22_date, "D").astype(int) - 30
    max_date = np.datetime64(q22_date, "D").astype(int) + 30

    date_dim_1part = date_dim_1part[
        (date_dim_1part["d_date"] >= min_date) and (date_dim_1part["d_date"] <= max_date)]

    output_table = output_table.merge(date_dim_1part, left_on=["inv_date_sk"],
                                      right_on=["d_date_sk"], how='inner',
                                      algorithm='sort', suffixes=('', ''))  # local 

    keep_columns = ["i_item_id", "inv_quantity_on_hand", "inv_warehouse_sk", "d_date"]
    output_table = output_table[keep_columns]

    output_table = output_table.merge(warehouse, left_on=["inv_warehouse_sk"],
                                      right_on=["w_warehouse_sk"], how='inner',
                                      algorithm='sort', suffixes=('', ''), env=env)

    keep_columns = ["i_item_id", "inv_quantity_on_hand",
                    "d_date", "w_warehouse_name"]
    output_table = output_table[keep_columns]

    d_date_int = np.datetime64(q22_date, "D").astype(int)

    output_table["inv_before"] = output_table["inv_quantity_on_hand"]
    output_table["inv_after"] = output_table["inv_quantity_on_hand"]

    # todo below 2 lines are not supported yet
    # output_table.loc[output_table["d_date"] >= d_date_int, "inv_before"] = 0
    # output_table.loc[output_table["d_date"] < d_date_int, "inv_after"] = 0

    keep_columns = ["i_item_id", "w_warehouse_name", "inv_before", "inv_after"]
    output_table = output_table[keep_columns]

    output_table = output_table.groupby(by=["w_warehouse_name", "i_item_id"], env=env) \
        .agg({"inv_before": "sum",
              "inv_after": "sum"})

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
    return output_table[0:100]  #todo use head operation here


if __name__ == "__main__":
    config = tpcxbb_argparser()

    mpi_config = MPIConfig()
    ctx: CylonEnv = CylonEnv(config=mpi_config, distributed=True)

    res = main(ctx, config)

    if ctx.rank == 0:
        import os

        os.makedirs(config['output_dir'], exist_ok=True)
        res.to_pandas().to_csv(f"{config['output_dir']}/q22_results.csv", index=False)

    ctx.finalize()
