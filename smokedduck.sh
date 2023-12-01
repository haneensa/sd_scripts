# smokedduck repo: https://anonymous.4open.science/r/duckdb-4BE5
python3 lineage_benchmark/micro_capture_run.py reproduce --enable_lineage --save_csv --csv_append
python3 lineage_benchmark/micro_capture_run.py reproduce --enable_lineage --stats --save_csv --csv_append

for sf in 1 10; do
  for n in 1 2 4; do
    python3 lineage_benchmark/tpch_benchmark.py  reproduce_sd --enable_lineage --threads ${n} --csv_append --save_csv --sf ${sf}
    python3 lineage_benchmark/tpch_benchmark.py  reproduce_sd --enable_lineage --stats  --threads ${n} --csv_append --save_csv --sf ${sf}
  done
done
