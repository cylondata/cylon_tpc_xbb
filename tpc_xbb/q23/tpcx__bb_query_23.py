import sys
sys.path.append("../../")
import os
from pycylon import CylonContext
from tpc_xbb.table_loader import data_dim, inventory



# x.split(/(d_[a-z_]+)/g).filter(x=>x.indexOf("d_")>-1).map(x=>"'"+x+"'").join(",")

data_root = os.environ['TPCX_BB_DATA_ROOT'] #"/home/chathura/code/benchmarks/bigbench/data/"

ctx: CylonContext = CylonContext(config=None, distributed=False)

date_dim_table = data_dim(data_root, ctx)
inventory_table = inventory(data_root, ctx)

print(inventory_table.column_names)

inventory_data_dim_joined = inventory_table.join(table=date_dim_table, join_type='inner', algorithm='sort', left_on=['inv_date_sk'], right_on=['d_date_sk'])

# print(inventory_table.shape)
