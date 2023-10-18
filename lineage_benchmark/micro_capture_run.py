# Benchmark DuckDB's physical operators
import duckdb
import pandas as pd
import argparse
import csv
import numpy as np

from utils import MicroDataZipfan, MicroDataSelective, DropLineageTables, Run, MicroDataMcopies
from micro_physical_op import *

parser = argparse.ArgumentParser(description='Micro Benchmark: Physical Operators')
# results management
parser.add_argument('notes', type=str,  help="run notes")
parser.add_argument('--save_csv', action='store_true',  help="save result in csv")
parser.add_argument('--csv_append', action='store_true',  help="Append results to old csv")
parser.add_argument('--show_output', action='store_true',  help="query output")
# lineage system
parser.add_argument('--enable_lineage', action='store_true',  help="Enable trace_lineage")
parser.add_argument('--stats', action='store_true',  help="get lineage size, nchunks and postprocess time")
parser.add_argument('--perm', action='store_true',  help="Use Perm Approach with join")
# benchmark setting
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=1)
parser.add_argument('--base', type=str, help="Base directory for benchmark_data", default="")
args = parser.parse_args()
args.profile = True # always true since we capture detailed profiling results
# append log results for each query instance
# schema:  ["query", "runtime", "cardinality", "groups", "output", "lineage_size", "lineage_type"]
results = []

if args.enable_lineage:
    lineage_type = "SD_full"
    if args.stats:
        lineage_type = "SD_stats"
elif args.perm:
    lineage_type = "Logical"
else:
    lineage_type = "Baseline"

print("Lineage Type: ", lineage_type, args.enable_lineage)
con = duckdb.connect(database=':memory:', read_only=False)

if args.perm and args.enable_lineage:
    args.enable_lineage=False


args.r = 3
if args.stats:
    args.r = 1
################### TODO: 
# 1. Check data exists if not, then generate data
folder = args.base + "benchmark_data/"
groups = [10, 50, 100, 1000, 10000]
cardinality = [1000, 10000, 100000, 1000000, 5000000, 10000000]
max_val = 100
a_list = [1, 0]
#MicroDataZipfan(folder, groups, cardinality, max_val, a_list)
selectivity = [0.0, 0.02, 0.2, 0.5, 0.8, 1.0]
cardinality = [1000000, 5000000, 10000000]
MicroDataSelective(folder, selectivity, cardinality)

################### Order By ###########################
##  order on 'z' with 'g' unique values and table size
#   of 'card' cardinality. Goal: see the effect of
#   large table size on lineage capture overhead
#   zipfan a=1 -> has duplicates
########################################################
groups = [100]
cardinality = [1000000, 5000000, 10000000]
ScanMicro(con, args, folder, lineage_type, groups, cardinality, results)
OrderByMicro(con, args, folder, lineage_type, groups, cardinality, results)

################### Filter ###########################
##  filter on 'z' with 'g' unique values and table size
#   of 'card' cardinality. Test on values on z with
#   different selectivity
#   TODO: specify data cardinality: [nothing, 50%, 100%]
########################################################
selectivity = [0.02, 0.2, 0.5, 1.0]
cardinality = [1000000, 5000000, 10000000]
pushdown = "filter"
FilterMicro(con, args, folder, lineage_type, selectivity, cardinality, results, pushdown)
pushdown = "clear"
FilterMicro(con, args, folder, lineage_type, selectivity, cardinality, results, pushdown)

################### Hash Aggregate  ############
##  Group by on 'z' with 'g' unique values and table size
#   of 'card'. Test on various 'g' values.
########################################################
groups = [10, 100, 1000]
cardinality = [1000000, 5000000, 10000000]
MicroDataZipfan(folder, groups, cardinality, max_val, a_list)
args.list = False
args.window = False
args.group_concat = False
agg_type = "PERFECT_HASH_GROUP_BY"
int_hashAgg(con, args, folder, lineage_type, groups, cardinality, results, agg_type)
agg_type = "HASH_GROUP_BY"
int_hashAgg(con, args, folder, lineage_type, groups, cardinality, results, agg_type)
if args.perm:
    agg_type = "HASH_GROUP_BY"
    args.list = True
    args.window = False
    args.group_concat = False
    int_hashAgg(con, args, folder, lineage_type, groups, cardinality, results, agg_type)
    args.list = False
    args.window = False
    args.group_concat = True
    int_hashAgg(con, args, folder, lineage_type, groups, cardinality, results, agg_type)

    args.list = False
    args.window = True
    args.group_concat = False
    int_hashAgg(con, args, folder, lineage_type, groups, cardinality, results, agg_type)


groups = [10, 100, 1000]
cardinality = [1000000, 5000000, 10000000]
hashAgg(con, args, folder, lineage_type, groups, cardinality, results)

################### Joins  ############
########################################################
print("------------ Test Joins-----------")

# 1k (10k, 100k, 1M)
cardinality = [(1000, 10000), (1000, 100000), (1000, 1000000)]
sels = [0.1, 0.2, 0.5, 0.8]
pred = " where t1.v < t2.v"
op = "PIECEWISE_MERGE_JOIN"
join_lessthan(con, args, folder, lineage_type, cardinality, results, op, True, pred, sels)

op = "NESTED_LOOP_JOIN"
join_lessthan(con, args, folder, lineage_type, cardinality, results, op, True, pred, sels)

pred = " where t1.v = t2.v or t1.v < t2.v"
op = "BLOCKWISE_NL_JOIN"
join_lessthan(con, args, folder, lineage_type, cardinality, results, op, False, pred, sels)

pred = ""
op = "CROSS_PRODUCT"
join_lessthan(con, args, folder, lineage_type, cardinality, results, op, False, pred)


############## PKFK ##########
groups = [10000, 100000]
cardinality = [1000000, 5000000, 10000000]
a_list = [0, 1]
MicroDataZipfan(folder, groups, cardinality, max_val, a_list)
op = "HASH_JOIN"
FKPK(con, args, folder, lineage_type, groups, cardinality, a_list, results, op,False)

op = "INDEX_JOIN"
index_scan = False
repeat = args.repeat
args.repeat = 1
FKPK(con, args, folder, lineage_type, groups, cardinality, a_list, results, op, index_scan)
args.repeat = repeat

index_scan = True
FKPK(con, args, folder, lineage_type, groups, cardinality, a_list, results, op, index_scan)

## ADD m:n
groups = [5, 10, 100]
a_list = [0, 0.5, 0.8, 1]
MicroDataZipfan(folder, groups, [1000], max_val, a_list)
cardinality = [10000, 100000, 1000000]
# number of matching elements n / g
MicroDataZipfan(folder, [10, 100], cardinality, max_val, a_list)
op = "HASH_JOIN"
MtM(con, args, folder, lineage_type, groups, cardinality, a_list, results, op,False)

groups = [10, 100]
cardinality = [10000, 100000]

op = "INDEX_JOIN"
repeat = args.repeat
args.repeat = 1
MtM(con, args, folder, lineage_type, groups, cardinality, a_list, results, op,False)
MtM(con, args, folder, lineage_type, groups, cardinality, a_list, results, op,True)
args.repeat = repeat



########### write results to csv
if args.save_csv:
    filename="micro_benchmark_notes_"+args.notes+".csv"
    PersistResults(results, filename, args.csv_append)

