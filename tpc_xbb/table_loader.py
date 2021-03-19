from pyarrow import csv
import pyarrow as pa
from pycylon import Table

parse_options = csv.ParseOptions(delimiter="|")


def data_dim(data_root, ctx, partition=""):
    data_dim_arrow_table = csv.read_csv(data_root+"/date_dim/date_dim"+partition+".dat", parse_options=csv.ParseOptions(delimiter="|"),
                                        convert_options=csv.ConvertOptions(
                                            column_types={'d_date_sk': pa.int64(), 'd_date': pa.string()}),
                                        read_options=csv.ReadOptions(column_names=['d_date_sk', 'd_date_id', 'd_date', 'd_month_seq', 'd_week_seq', 'd_quarter_seq', 'd_year', 'd_dow', 'd_moy', 'd_dom', 'd_qoy', 'd_fy_year', 'd_fy_quarter_seq', 'd_fy_week_seq', 'd_day_name', 'd_quarter_name', 'd_holiday', 'd_weekend', 'd_following_holiday', 'd_first_dom', 'd_last_dom', 'd_same_day_ly', 'd_same_day_lq', 'd_current_day', 'd_current_week', 'd_current_month', 'd_current_quarter', 'd_current_year']))

    return Table.from_arrow(ctx, data_dim_arrow_table)


def inventory(data_root, ctx, partition=""):
    inventory_arrow_table = csv.read_csv(data_root+"/inventory/inventory"+partition+".dat", parse_options=csv.ParseOptions(delimiter="|"),
                                         convert_options=csv.ConvertOptions(
                                             column_types={'inv_date_sk': pa.int64(), 'inv_item_sk': pa.int64(), 'inv_warehouse_sk': pa.int64()}),
                                         read_options=csv.ReadOptions(column_names=['inv_date_sk', 'inv_item_sk', 'inv_warehouse_sk', 'inv_quantity_on_hand']))
    return Table.from_arrow(ctx, inventory_arrow_table)
