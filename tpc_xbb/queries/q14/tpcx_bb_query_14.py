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


import numpy as np
import sys

from tpc_xbb.tools.readers import CSVReader
from tpc_xbb.tools.utils import (
    tpcxbb_argparser
)


def read_tables(ctx, config):
    table_reader = CSVReader(config["data_dir"],
                             rank=None if ctx.get_rank() == 1 else ctx.get_rank())

    ws_columns = ["ws_ship_hdemo_sk", "ws_web_page_sk", "ws_sold_time_sk"]
    web_sales = table_reader.read(ctx, "web_sales", relevant_cols=ws_columns)

    hd_columns = ["hd_demo_sk", "hd_dep_count"]
    household_demographics = table_reader.read(ctx,
                                               "household_demographics", relevant_cols=hd_columns
                                               )

    wp_columns = ["wp_web_page_sk", "wp_char_count"]
    web_page = table_reader.read(ctx, "web_page", relevant_cols=wp_columns)

    td_columns = ["t_time_sk", "t_hour"]
    time_dim = table_reader.read(ctx, "time_dim", relevant_cols=td_columns)

    return web_sales, household_demographics, web_page, time_dim


def main(ctx, config):
    q14_dependents = 5
    q14_morning_startHour = 7
    q14_morning_endHour = 8
    q14_evening_startHour = 19
    q14_evening_endHour = 20
    q14_content_len_min = 5000
    q14_content_len_max = 6000

    web_sales, household_demographics, web_page, time_dim = read_tables(
        ctx, config)

    # print("web sales")
    # print(web_sales[0:10])

    # print("household_demographics")
    # print(household_demographics[0:10])

    # print("web page")
    # print(web_page[0:10])

    # print("time_dim")
    # print(time_dim[0:10])

    household_demographics = household_demographics[
        ("hd_dep_count" == q14_dependents)
        # meta=household_demographics._meta,
        # local_dict={"q14_dependents": q14_dependents},
    ]

    output_table = web_sales.join(
        household_demographics,
        left_on=["ws_ship_hdemo_sk"],
        right_on=["hd_demo_sk"],
        join_type="inner",
        algorithm="sort"
    )

    # print("After join")
    # print(output_table[0:10])

    output_table = output_table.drop(
        ["ws_ship_hdemo_sk", "hd_demo_sk", "hd_dep_count"])

    web_page = web_page[
        (web_page["wp_char_count"] >= q14_content_len_min) &
        (web_page["wp_char_count"] >= q14_content_len_min) &
        (web_page["wp_char_count"] <= q14_content_len_max) &
        (web_page["wp_char_count"] >= q14_content_len_min) &
        (web_page["wp_char_count"] <= q14_content_len_max)]

    output_table = output_table.join(
        web_page, left_on=["ws_web_page_sk"], right_on=["wp_web_page_sk"], join_type="inner",
        algorithm="sort"
    )

    output_table = output_table.drop(
        ["ws_web_page_sk", "wp_web_page_sk", "wp_char_count"])

    time_dim = time_dim[
        (time_dim["t_hour"] == q14_morning_startHour) |
        (time_dim["t_hour"] == q14_morning_endHour) |
        (time_dim["t_hour"] == q14_evening_startHour) |
        (time_dim["t_hour"] == q14_evening_endHour)
        ]

    output_table = output_table.join(
        time_dim, left_on=["ws_sold_time_sk"], right_on=["t_time_sk"], join_type="inner",
        algorithm="sort"
    )

    output_table = output_table.drop(["ws_sold_time_sk", "t_time_sk"])

    output_table["am"] = (output_table["t_hour"] >= q14_morning_startHour) & (
            output_table["t_hour"] <= q14_morning_endHour
    )

    output_table["pm"] = (output_table["t_hour"] >= q14_evening_startHour) & (
            output_table["t_hour"] <= q14_evening_endHour
    )

    # print("output_table")
    # print(output_table[0:10])

    am_trues = output_table[output_table["am"] == True]
    pm_trues = output_table[output_table["pm"] == True]

    am_pm_ratio = am_trues.row_count / pm_trues.row_count

    if np.isinf(am_pm_ratio):
        am_pm_ratio = -1.0

    print(am_pm_ratio)


if __name__ == "__main__":
    config = tpcxbb_argparser()

    from pycylon import CylonContext
    from pycylon.net import MPIConfig

    mpi_config = MPIConfig()
    ctx: CylonContext = CylonContext(config=mpi_config, distributed=True)

    main(ctx, config)
