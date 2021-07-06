#
# Copyright (c) 2019-2020, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# use query 6

import sys

from typing import Iterable
import cudf
import cupy
import time
import gc

import pygcylon as gcy


def get_size(df_size):
    if df_size.endswith("MB"):
        df_size = df_size[:-2]
        df_size = int(df_size) * 1000000
        return df_size
    elif df_size.endswith("GB"):
        df_size = df_size[:-2]
        df_size = int(df_size) * 1000000000
        return df_size
    else:
        raise ValueError("Size has to be either MB or GB")


def create_df(df_size, ncols, nworkers):
    # index is an additional column
    nrows = int(df_size / ((ncols + 1) * nworkers * 8))
    df = cudf.DataFrame()
    for i in range(ncols):
        df[str(i)] = cupy.arange(0, nrows)
    return gcy.DataFrame.from_cudf(df)


def main(env: gcy.CylonEnv, df_size, ncols):

    df = create_df(df_size, ncols, env.world_size)

    print(env.rank, "number of rows in df:", len(df.to_cudf().index))
    memuse = df.to_cudf().memory_usage(deep=True) / 1000000
    print(env.rank, "columns sizes of initial df in MB:", memuse)
    print(env.rank, "total size of initial df in MB:", memuse.sum())
    print()

    if env.rank == 0:
        print(env.rank, "columns in df:", df.to_cudf().columns)
        print()

    # shuffle on the first column
    # first column index = number of indices column
    gc.disable()
    t0 = time.time()
    shuffled_df = gcy.shuffle(df._cdf, [df._cdf._num_indices], env=env)
    shuffle_time = (time.time() - t0) * 1000
    gc.enable()

    print(env.rank, "number of rows in shuffled_df:", len(shuffled_df.index))
    memuse = shuffled_df.memory_usage(deep=True) / 1000000
    print(env.rank, "total size of shuffled_df in MB:", memuse.sum())
    print()

    return shuffle_time


if __name__ == "__main__":

    if len(sys.argv) < 2:
        raise ValueError("Size of the data to generate must be given as the first parameter as 12MB or 23GB")

    total_size = get_size(sys.argv[1])
    # number of columns
    number_of_cols = 3

    mpi_config = gcy.MPIConfig()
    env: gcy.CylonEnv = gcy.CylonEnv(config=mpi_config, distributed=True)

    # set the gpu
    localGpuCount = cupy.cuda.runtime.getDeviceCount()
    deviceID = env.rank % localGpuCount
#    deviceID = (env.rank + 4) % localGpuCount
#    deviceID = 0
    cupy.cuda.Device(deviceID).use()
    print("gpu in use:", cupy.cuda.runtime.getDevice(), "by the worker:", env.rank)

    shuffle_time = main(env, total_size, number_of_cols)

    # write the result to results.txt file
    output_file = "single_run_" + str(env.rank) + ".csv"
    f = open(output_file, "w")
    f.write(f"{env.rank},{shuffle_time}\n")
    f.close()
    print("rank:", env.rank, shuffle_time)

    env.finalize()
