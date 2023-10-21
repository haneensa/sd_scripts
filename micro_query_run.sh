python3 lineage_benchmark/micro_querying_run.py --op hashjoin --num_oids 1000
python3 lineage_benchmark/micro_querying_run.py --op mergejoin --num_oids 1000
python3 lineage_benchmark/micro_querying_run.py --op nljoin --num_oids 1000
python3 lineage_benchmark/micro_querying_run.py --op groupby --num_oids 1000 --fixed 1
python3 lineage_benchmark/micro_querying_run.py --op perfgroupby --num_oids 1000 --fixed 1
python3 lineage_benchmark/micro_querying_run.py --op simpleagg --num_oids 1 --fixed 1
python3 lineage_benchmark/micro_querying_run.py --op orderby --num_oids 1000
python3 lineage_benchmark/micro_querying_run.py --op filter --num_oids 1000
