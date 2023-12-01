import numpy as np
import pandas as pd
import duckdb
import json
import os

def find_node_wprefix(prefix, plan):
    op_name = None
    if prefix[-4:] == "_mtm":
        prefix = prefix[:-4]
    for k, v in plan.items():
        if k[:len(prefix)] == prefix:
            op_name = k
            break
    assert op_name is not None, f"Can't find {prefix}"
    return op_name

def getOperatorRuntime(plan, op, x):
    """
    given a query plan and an operator from duckdb,
    return the operator's runtime in the query plan
    """
    plan= plan.replace("'", "\"")
    plan = json.loads(plan)
    if x["lineage_type"] == "Logical_window":
        op = "WINDOW"
    op = find_node_wprefix(op, plan)
    
    assert op in plan, f"ERROR {op} {plan}"
    return plan[op]

def getAllExec(plan, op):
    """
    return execution time (sec) from profiling
    data stored in query plan
    """
    plan = plan.replace("'", "\"")
    plan = json.loads(plan)
    total = 0.0
    for k, v in plan.items():
        total += float(v)
    return total

def getMat(plan, op):
    """
    return materialization time (sec) from
    profiling data stored in query plan
    """
    plan= plan.replace("'", "\"")
    plan = json.loads(plan)
    op = find_node_wprefix("CREATE_TABLE_AS", plan)
    return plan[op]

def fanout(a, b):
    if b == 0: return 0
    return a / b
    
# TODO: add p column
def get_p(row):
    if row["query"] in ["FILTER", "SEQ_SCAN", "SEQ", "ORDER_BY"]:
        return row["ncol"]+3
    else:
        return row["ncom"]

# TODO: add sel column
def get_sel(row):
    if row["query"] == "CROSS_PRODUCT" or row["query"][-4:] == "JOIN" or row["query"][-3:]=="mtm":
        return round(1.0 - float(row["groups"].split(",")[1]), 1)
    elif row["query"] in ["FILTER", "SEQ_SCAN"]:
        return row["groups"]
    else:
        return 1.0

# TODO: add index_scan column
def get_index_join_qtype(row):
    if row["query"] in ["INDEX_JOIN", "INDEX_JOIN_mtm"]:
        if row["index_scan"] == False:
            return "Q_P,F"
        else:
            return "Q_P"
    else:
        return "-"

def unify_bool_type(row):
    if row["query"] in ["INDEX_JOIN", "INDEX_JOIN_mtm", "HASH_JOIN", "HASH_JOIN_mtm"]:
        if row["index_scan"] == True or row["index_scan"] == "True":
            return True
        else:
            return False
    else:
        return False
def set_to_none(row):
    if row["query"] in ["INDEX_JOIN", "INDEX_JOIN_mtm", "HASH_JOIN", "HASH_JOIN_mtm"]:
        return -1
    else:
        return row["sel"]

def getstats(row, i):
    stats = row["stats"]
    if type(stats) == str and len(stats.split(",")) > i:
        stats = stats.split(",")
        if i == 0 and len(stats)==5:
            return stats[4]
        return stats[i]
    else:
        0

def preprocess(con, result_file):
    """
    lineage_type: SD_full, SD_copy, SD_stats, Logical, Baseline
    TODO: don't depend on notes
    """
    df = pd.read_csv(result_file)
    
    df.loc[:, "lineage_size"] = df.apply(lambda x: getstats(x, 0), axis=1)
    df.loc[:, "lineage_count"] = df.apply(lambda x: getstats(x, 1), axis=1)
    df.loc[:, "nchunks"] = df.apply(lambda x: getstats(x, 2), axis=1)
    df.loc[:, "postprocess"] = df.apply(lambda x: getstats(x, 3), axis=1)

    df.loc[:, "op_runtime"] = df.apply(lambda x: getOperatorRuntime(x['plan_timings'], x['query'], x), axis=1)
    df.loc[:, "mat_time"] = df.apply(lambda x: getMat(x['plan_timings'], x['query']), axis=1)
    df.loc[:, "plan_runtime"] = df.apply(lambda x: getAllExec(x['plan_timings'], x['query']), axis=1)
    
    df.loc[:, "index_scan"] = df.apply(lambda x: unify_bool_type(x), axis=1)
    df.loc[:, "index_join"] = df.apply(lambda x: get_index_join_qtype(x), axis=1)
    df.loc[:, "sel"] = df.apply(lambda x: set_to_none(x), axis=1)
    df_final = df

    metrics = ["runtime", "output", "op_runtime", "mat_time", "plan_runtime", "nchunks", "lineage_size", "lineage_count", "postprocess"]
    header_unique = ["query", "n1", "n2", "skew", "ncol", "sel", "groups", "r", "index_join", "lineage_type"]

    header = header_unique + metrics
    processed_data = pd.DataFrame(df_final[header]).reset_index(drop=True)
    file_name = "micro.csv"
    processed_data = processed_data.fillna(-1)
    processed_data.to_csv(file_name, encoding='utf-8',index=False)
    header_unique = ["query", "n1", "n2", "skew", "ncol", "sel", "groups",  "index_join", "lineage_type"]
    g = ','.join(header_unique)
    #print(g)
    con.execute("CREATE TABLE micro_test AS SELECT * FROM '{}';".format(file_name))
    con.execute("COPY (SELECT * FROM micro_test) TO '{}' WITH (HEADER 1, DELIMITER '|');".format(file_name))
    #print(con.execute("pragma table_info('micro_test')").fetchdf())
    #print("*** ", con.execute("select distinct(query) from micro_test").fetchdf())
    # average over the different runs (r)
    # plan_runtime: runtime of all physical operators including create table operator
    # runtime: user runtime
    # output: size of the output
    # op_runtime: runtime of the specific physical operator 'query'
    # mat_runtime: create table runtime
    print(con.execute("""create table avg_micro as
                        select {}, max(output) as output,
                                max(nchunks) as nchunks,
                                max(lineage_size) as lineage_size, max(lineage_count) as lineage_count,
                                max(postprocess) as postprocess,
                                avg(plan_runtime) as plan_runtime, avg(runtime) as runtime,
                                avg(output) as output, avg(op_runtime) as op_runtime, avg(mat_time) as mat_time from micro_test
                                group by {}""".format(g, g)).fetchdf())
    df = con.execute(f"""select {g} from avg_micro order by n1, n2, groups, index_join""").fetchdf()
    print(" *****>")
    print(df[df["query"]=="INDEX_JOIN_mtm"])
    header_unique.remove("lineage_type")
    g = ','.join(header_unique)
    m = ','.join(metrics)
    print(g)
    print(m)
    print(con.execute(f"""create table micro_withBaseline as select t1.plan_runtime as base_plan_runtime, t1.runtime as base_runtime,
                      t1.output as base_output, t1.op_runtime as base_op_runtime, t1.mat_time as base_mat_time,
                      (t1.plan_runtime-t1.mat_time) as base_plan_no_create,
                      (t2.plan_runtime-t2.mat_time) as plan_no_create,
                      t2.* from (select {g}, {m} from avg_micro where lineage_type='Baseline') as t1 join avg_micro as t2 using ({g})
                      """))
    df = con.execute(f"""select {m}, {g}, lineage_type from micro_withBaseline order by n1, n2, groups, index_join""").fetchdf()
    print(df)
    print(" *****>")
    print(df[df["query"]=="INDEX_JOIN_mtm"])
    """
    Relative overhead: percentage increase in execution time  caused by the operation  compared to some baseline.
    sys (uint) execution of system of interest
    baseSys (uint) baseline execution
    base (uint) runtime of baseline execution

    (sys-baseSys) -> overhead of system compared to baseline
    h = max(((sys-baseSys)/base)*100, 0)
    """

    # 1. all_ops_no_mat_{r}overhead: overhead on all operators excluding
    # 2. mat_{r}overhead: materialization overhead (SD overhead of resizing the log, Perm: overhead of materialization annotations)
    # 3. op_no_mat_{r}overhead: overhead of isolated operator
    # 4. total overhead: all_ops_no_mat + mat

    #((plan_runtime-base_plan_runtime)-(mat_time-base_mat_time))*1000 as exec_overhead,
    #(((plan_runtime - mat_time) - (base_plan_runtime - base_mat_time))/base_op_runtime)*100 as exec_rel_overhead,
    #((plan_runtime-mat_time) - (base_plan_runtime-base_mat_time))*1000 as query_overhead,
    #((plan_runtime-mat_time) - (base_runtime-base_mat_time))/(base_runtime-base_mat_time)*1000 as query_roverhead,
    # calculate metrics for Perm
    print(con.execute("""create table micro_perm_metrics as select {}, lineage_type, output,
                                nchunks, lineage_size, lineage_count, postprocess,
                                0 as postprocess_roverhead,
                                (plan_no_create-base_plan_no_create)*1000 as plan_execution_overhead,
                                ((plan_no_create-base_plan_no_create)/base_plan_no_create)*100 as plan_execution_roverhead,
                                
                                (mat_time - base_mat_time)*1000 as plan_mat_overhead,
                                ((mat_time - base_mat_time) / base_plan_no_create) *100 as plan_mat_roverhead,

                                (plan_runtime-base_plan_runtime)*1000 as plan_all_overhead,
                                ((plan_runtime-base_plan_runtime)/base_plan_no_create)*100 as plan_all_roverhead,
                                

                                (runtime - base_runtime)*1000 as overhead,
                                ((op_runtime - base_op_runtime)/base_op_runtime)*100 as rel_overhead,
                                (op_runtime-base_op_runtime)*1000 as exec_overhead,
                                (((op_runtime - base_op_runtime))/base_op_runtime)*100 as exec_roverhead,

                                ((plan_runtime-base_plan_runtime))*1000 as total_overhead,
                                output / base_output as fanout
                      from micro_withBaseline
                      where lineage_type='Logical' or lineage_type='Logical_list' or lineage_type='Logical_group_concat' or lineage_type='Logical_window'  """.format(g)).fetchdf())

    print(con.execute("select * from micro_perm_metrics").fetchdf())
    #((sd_copy.op_runtime-sd_copy.base_op_runtime))*1000 as exec_overhead,
    #((sd_copy.op_runtime-sd_copy.base_op_runtime)/sd_copy.base_op_runtime)*100 as exec_roverhead,
    # calculate metrics for SmokedDuck
    # 1. op_sd_copy time (op_execution) -- overhead on operators we are inspecting excluding log.push and create table
    # 2. op_sd_mat time (ap_mat)  -- overhead of create table and log.push for single op
    # 3. op_sd_full time (op_all) -- overhead of all including materialization and log.push

    # 4. plan_copy time (plan_execution) -- overhead on all operators except create_table for Perm and log.push_back()
    # 5. plan_mat time (plan_mat) -- overhead of materialization on all operators (SD spread, Perm create table)
    # 6. plan_full time (plan_all) -- overhead of create table and log.push_back()
    con.execute("""create table micro_sd_metrics as select {}, 'SD' as lineage_type, sd_full.output,
sd_stats.nchunks, sd_stats.lineage_size, sd_stats.lineage_count, sd_stats.postprocess,
(sd_stats.postprocess / (sd_full.base_plan_no_create*1000)) as postprocess_roverhead,
(sd_copy.plan_no_create-sd_copy.base_plan_no_create)*1000 as plan_execution_overhead,
((sd_copy.plan_no_create-sd_copy.base_plan_no_create)/sd_copy.base_plan_no_create)*100 as plan_execution_roverhead,
                                
(sd_full.plan_no_create - sd_copy.plan_no_create)*1000 as plan_mat_overhead,
((sd_full.plan_no_create - sd_copy.plan_no_create)/sd_copy.plan_no_create)*100 as plan_mat_roverhead,
                                
case
when sd_full.base_plan_no_create > sd_full.plan_no_create then
0
else
(sd_full.plan_no_create-sd_full.base_plan_no_create)*1000
end
as plan_all_overhead,

case
when sd_full.base_plan_no_create > sd_full.plan_no_create then
0
else
((sd_full.plan_no_create-sd_full.base_plan_no_create)/sd_full.base_plan_no_create)*100
end
as plan_all_roverhead,
                                
(sd_full.op_runtime - sd_full.base_op_runtime)*1000 as overhead,
((sd_full.op_runtime - sd_full.base_op_runtime)/sd_full.base_op_runtime) * 100 as rel_overhead,

((sd_full.op_runtime-sd_full.base_op_runtime))*1000 as exec_overhead,
((sd_full.op_runtime-sd_full.base_op_runtime)/sd_full.base_op_runtime)*100 as exec_roverhead,

(sd_full.op_runtime - sd_full.base_op_runtime)*1000 as total_overhead,
sd_full.output / sd_full.base_output as fanout
                         from (select * from micro_withBaseline where lineage_type='SD_copy') as sd_copy JOIN
                              (select * from micro_withBaseline where lineage_type='SD_full') as sd_full
                              USING ({}) JOIN
                              (select * from micro_withBaseline where lineage_type='SD_stats') as sd_stats
                              USING ({})
                      """.format(g, g, g)).fetchdf()
    print(con.execute("select * from  micro_sd_metrics where query='NESTED_LOOP_JOIN'").fetchdf())

def get_db():
    # check if micro.db exists, if not, then reconstruct the database
    database = "micro.db"
    if os.path.exists(database):
        con = duckdb.connect(database=database, read_only=False)
    else:
        con = duckdb.connect(database=database, read_only=False)
        result_file = "micro_benchmark_notes_reproduce.csv"
        preprocess(con, result_file)
    return con

#get_db()
