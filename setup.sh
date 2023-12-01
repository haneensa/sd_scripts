touch tpch_benchmark_capture_reproduce_logical.csv
echo 'query,runtime,distinct_runtime,sf,repeat,lineage_type,n_threads,distinct_output,output,stats,notes,plan_timings' > tpch_benchmark_capture_reproduce_logical.csv

touch tpch_benchmark_capture_reproduce_sd.csv
echo 'query,runtime,distinct_runtime,sf,repeat,lineage_type,n_threads,distinct_output,output,stats,notes,plan_timings' > tpch_benchmark_capture_reproduce_sd.csv

touch micro_benchmark_notes_reprodce.csv
echo 'r,query,runtime,n1,n2,sel,skew,ncol,groups,index_scan,output,stats,lineage_type,notes,plan_timings' >  micro_benchmark_notes_reprodce.csv
