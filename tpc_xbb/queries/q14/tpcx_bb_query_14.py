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

import numpy as np
import sys

from cylon_xbb_tools.readers import CSVReader
from cylon_xbb_tools.utils import (
    tpcxbb_argparser
)

from pycylon.net import MPIConfig
from pycylon import CylonEnv, DataFrame


def read_tables(env: CylonEnv, config) -> Iterable[DataFrame]:
    table_reader = CSVReader(config["data_dir"], rank=None if env.world_size == 1 else env.rank)

    ws_columns = ["ws_ship_hdemo_sk", "ws_web_page_sk", "ws_sold_time_sk"]
    web_sales = table_reader.read(env, "web_sales", relevant_cols=ws_columns)

    hd_columns = ["hd_demo_sk", "hd_dep_count"]
    household_demographics_1part = table_reader.read(env, "household_demographics",
                                                     relevant_cols=hd_columns)

    wp_columns = ["wp_web_page_sk", "wp_char_count"]
    web_page = table_reader.read(env, "web_page", relevant_cols=wp_columns)

    td_columns = ["t_time_sk", "t_hour"]
    time_dim_1part = table_reader.read(env, "time_dim", relevant_cols=td_columns)

    return web_sales, household_demographics_1part, web_page, time_dim_1part


def main(env: CylonEnv, config):
    q14_dependents = 5
    q14_morning_startHour = 7
    q14_morning_endHour = 8
    q14_evening_startHour = 19
    q14_evening_endHour = 20
    q14_content_len_min = 5000
    q14_content_len_max = 6000

    web_sales, household_demographics_1part, web_page, time_dim_1part = read_tables(
        env, config)

    # print("web sales")
    # print(web_sales[0:10])

    # print("household_demographics")
    # print(household_demographics[0:10])

    # print("web page")
    # print(web_page[0:10])

    # print("time_dim")
    # print(time_dim[0:10])

    household_demographics_1part = household_demographics_1part[
        ("hd_dep_count" == q14_dependents)
        # meta=household_demographics._meta,
        # local_dict={"q14_dependents": q14_dependents},
    ]

    output_table = web_sales.merge(household_demographics_1part, how="inner", algorithm="sort",
                                   left_on=["ws_ship_hdemo_sk"], right_on=["hd_demo_sk"],
                                   suffixes=('', ''))  # local

    # print("####", output_table.row_count)

    # print("After join")
    # print(output_table[0:10])

    output_table = output_table.drop(["ws_ship_hdemo_sk", "hd_demo_sk", "hd_dep_count"])

    web_page = web_page[(web_page["wp_char_count"] >= q14_content_len_min) &
                        (web_page["wp_char_count"] >= q14_content_len_min) &
                        (web_page["wp_char_count"] <= q14_content_len_max) &
                        (web_page["wp_char_count"] >= q14_content_len_min) &
                        (web_page["wp_char_count"] <= q14_content_len_max)]

    output_table = output_table.merge(web_page, how="inner", algorithm="sort",
                                      left_on=["ws_web_page_sk"], right_on=["wp_web_page_sk"],
                                      suffixes=('', ''), env=env)

    output_table = output_table.drop(["ws_web_page_sk", "wp_web_page_sk", "wp_char_count"])

    time_dim_1part = time_dim_1part[(time_dim_1part["t_hour"] == q14_morning_startHour) |
                                    (time_dim_1part["t_hour"] == q14_morning_endHour) |
                                    (time_dim_1part["t_hour"] == q14_evening_startHour) |
                                    (time_dim_1part["t_hour"] == q14_evening_endHour)]

    output_table = output_table.merge(time_dim_1part, how="inner", algorithm="sort",
                                      left_on=["ws_sold_time_sk"], right_on=["t_time_sk"],
                                      suffixes=('', ''))  # local

    output_table = output_table.drop(["ws_sold_time_sk", "t_time_sk"])

    output_table["am"] = (output_table["t_hour"] >= q14_morning_startHour) & \
                         (output_table["t_hour"] <= q14_morning_endHour)

    output_table["pm"] = (output_table["t_hour"] >= q14_evening_startHour) & \
                         (output_table["t_hour"] <= q14_evening_endHour)

    # print("output_table")
    # print(output_table[0:10])

    am_trues = output_table[output_table["am"] == True]
    pm_trues = output_table[output_table["pm"] == True]

    # get the distributed count of these tables
    am_pm_ratio = am_trues.to_table().count(0) / pm_trues.to_table().count(0) # table API

    np_am_pm_ratio = am_pm_ratio.to_numpy()[0,0]
    if np.isinf(np_am_pm_ratio):
        np_am_pm_ratio = -1.0

    print(np_am_pm_ratio)

    from pycylon import Table
    return Table.from_pydict(env, {"am_pm_ratio": [np_am_pm_ratio]})


if __name__ == "__main__":
    config = tpcxbb_argparser()

    mpi_config = MPIConfig()
    ctx: CylonEnv = CylonEnv(config=mpi_config, distributed=True)

    res = main(ctx, config)

    if ctx.rank == 0:
        import os

        os.makedirs(config['output_dir'], exist_ok=True)
        res.to_pandas().to_csv(f"{config['output_dir']}/q14_results.csv", index=False)

    ctx.finalize()
