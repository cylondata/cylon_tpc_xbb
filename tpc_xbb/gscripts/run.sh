rm single_run_results.csv

nworkers=1

# mpirun -n $nworkers --hostfile hostfile \
#	--mca btl_openib_want_cuda_gdr 1 \
#	--mca btl_smcuda_cuda_ipc_verbose 100 \
#	-x UCX_TLS=rc,sm,cuda_copy,cuda_ipc \

mpirun -n $nworkers \
	--mca pml ucx \
	--mca btl_openib_allow_ib true \
	python queries/q06/gc_tpcx_bb_query_06.py \
	--config_file=config/benchmark_config.yaml

sleep 1

python gscripts/compute_result.py $nworkers 

