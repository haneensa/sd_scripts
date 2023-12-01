#!/bin/bash
# duckdb without instrumentation: https://anonymous.4open.science/r/duckdb-1853


python3 lineage_benchmark/micro_capture_run.py reproduce --save_csv --csv_append
python3 lineage_benchmark/micro_capture_run.py reproduce --perm --save_csv --csv_append
 
for sf in 1; do
  for n in 1 2 4; do
    python3 lineage_benchmark/tpch_benchmark.py  reproduce_logical --threads ${n} --csv_append --save_csv --sf ${sf}
    python3 lineage_benchmark/tpch_benchmark.py  reproduce_logical --perm --threads ${n} --csv_append --save_csv --sf ${sf}
  done
done
