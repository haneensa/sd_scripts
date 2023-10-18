from itertools import product
import random
import os.path
import duckdb
import pandas as pd
import argparse
import csv
import json

import numpy as np

from utils import getStats, MicroDataZipfan, MicroDataSelective, DropLineageTables, MicroDataMcopies,  Run

def fill_results(r, query, runtime, n1, n2, sel, skew, ncol, groups,
        index_scan, output, stats, lineage_type, notes, plan_timings):
    return [r, query, runtime, n1, n2, sel, skew, ncol, groups,
    index_scan, output, stats, lineage_type, notes, plan_timings]


def getStatsWrap(con, q, args, mem):
    stats = None
    if args.enable_lineage and args.stats:
        lineage_size, lineage_count, nchunks, postprocess_time= getStats(con, q)
        stats = "{},{},{},{},{}".format(lineage_size, lineage_count, nchunks, postprocess_time*1000, mem)
    else:
        stats = f"{mem}"
    return stats

def parse_plan_timings(qid):
    plan_fname = '{}_plan.json'.format(qid)
    plan_timings = {}
    with open(plan_fname, 'r') as f:
        plan = json.load(f)
        print(plan)
        plan_timings = gettimings(plan, {})
        print('X', plan_timings)
    os.remove(plan_fname)
    return plan_timings

def gettimings(plan, res={}):
    for c in  plan['children']:
        op_name = c['name']
        timing = c['timing']
        res[op_name + str(len(res))] = timing
        gettimings(c, res)
    return res

def PersistResults(results, filename, append):
    print("Writing results to ", filename, " Append: ", append)
    header = ['r', "query", "runtime", "n1", "n2", "sel", "skew", "ncol", "groups", "index_scan", "output", "stats", "lineage_type", "notes", "plan_timings"]
    control = 'w'
    if append:
        control = 'a'
    with open(filename, control) as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(results)

################### Order By ###########################
##  order on 'z' with 'g' unique values and table size
#   of 'card' cardinality. Goal: see the effect of
#   large table size on lineage capture overhead
########################################################

def ScanMicro(con, args, folder, lineage_type, groups, cardinality, results):
    print("------------ Test Scan zipfan 1", lineage_type)
    projections = [0, 2, 4, 8] 

    for card, g, p in product(cardinality, groups, projections):
        filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
        zipf1 = pd.read_csv(folder+filename)
        for col in range(p):
            zipf1["col{}".format(col)]  = np.random.randint(0, 100, len(zipf1))
        perm_rid = ''
        if args.perm:
            perm_rid = 'zipf1.rowid as rid,'

        con.register('zipf1_view', zipf1)
        con.execute("create table zipf1 as select * from zipf1_view")
        args.qid='Scan_ltype{}g{}card{}p{}'.format(lineage_type,g, card, p)
        for r in range(args.r):
            print(filename, p, g, card)
            q = "SELECT {}* FROM zipf1".format(perm_rid)
            q = "create table zipf1_perm_lineage as "+ q
            table_name='zipf1_perm_lineage'
            avg, df, mem = Run(q, args, con, table_name)
            df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
            output_size = df.loc[0,'c']
            con.execute("drop table zipf1_perm_lineage")
            stats = getStatsWrap(con, q, args, mem)
            plan_timings = parse_plan_timings(args.qid)
            results.append(fill_results(r=r, query="SEQ", runtime=avg, n1=card, n2=None, sel=None, skew=None, ncol=p, groups=g,
                    index_scan=None, output=output_size, stats=stats, lineage_type=lineage_type, notes=args.notes, plan_timings=plan_timings))
            if args.enable_lineage:
                DropLineageTables(con)
        con.execute("drop table zipf1")

def OrderByMicro(con, args, folder, lineage_type, groups, cardinality, results):
    print("------------ Test Order By zipfan 1", lineage_type)
    projections = [0, 2, 4, 8] 
    for g, card, p in product(groups, cardinality, projections):
        args.qid='OB_ltype{}g{}card{}p{}'.format(lineage_type,g, card, p)
        filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
        zipf1 = pd.read_csv(folder+filename)
        proj_ids = 'idx'
        for col in range(p):
            zipf1["col{}".format(col)]  = np.random.randint(0, 100, len(zipf1))
        perm_rid = ''
        if args.perm:
            perm_rid = 'zipf1.rowid as rid,'

        con.register('zipf1_view', zipf1)
        con.execute("create table zipf1 as select * from zipf1_view")
        for r in range(args.r):
            print(r, filename, p, g, card)
            q = "SELECT {}* FROM zipf1 Order By z".format(perm_rid)
            table_name = None
            q = "create table zipf1_perm_lineage as "+ q
            table_name='zipf1_perm_lineage'
            avg, df, mem = Run(q, args, con, table_name)
            df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
            output_size = df.loc[0,'c']
            con.execute("drop table zipf1_perm_lineage")
            stats = getStatsWrap(con, q, args, mem)
            plan_timings = parse_plan_timings(args.qid)
            results.append(fill_results(r=r, query="ORDER_BY", runtime=avg, n1=card, n2=None, sel=None, skew=None, ncol=p, groups=g,
                    index_scan=None, output=output_size, stats=stats, lineage_type=lineage_type, notes=args.notes, plan_timings=plan_timings))
            if args.enable_lineage:
                DropLineageTables(con)
        con.execute("drop table zipf1")

def setup_filter(con, tname: str, lineage_type: str, sel: float, card: int, p: int, folder: str):
    print("setup_filter: ", lineage_type, p, sel, card)
    # setup datasets
    filename = f"filter_sel{sel}_card{card}.csv"
    t1 = pd.read_csv(folder+filename)
    for col in range(p):
        t1["col{}".format(col)]  = np.random.randint(0, 100, len(t1))
    con.register('t1_view', t1)
    con.execute(f"create table {tname} as select * from t1_view")

def core_filter(con, args):
    # setup perm annotations
    perm_rid = ''
    if args.perm:
        perm_rid = 't1.rowid as rid,'

    q = "SELECT {}* FROM t1 where z=0".format(perm_rid)
    table_name = None
    q = "create table t1_perm_lineage as "+ q
    table_name='t1_perm_lineage'
    avg, df, mem = Run(q, args, con, table_name)
    df = con.execute("select count(*) as c from t1_perm_lineage").fetchdf()
    output_size = df.loc[0,'c']
    con.execute("drop table t1_perm_lineage")
    return q, output_size, avg, mem

################### Filter ###########################
# predicate: z=0
# vary: cardinality, selectivity, number of projected columns
# operators: Filter (FILTER), and Scan with filter push down (SEQ_SCAN)
########################################################
def FilterMicro(con, args, folder, lineage_type, selectivity, cardinality, results, pushdown):
    print("------------ Test Filter zipfan 1 ", lineage_type, " filter_pushdown: ", pushdown)
    con.execute("PRAGMA set_filter='{}'".format(pushdown))
    projections = [0, 2, 4, 8] 
    
    name = "FILTER"
    if (pushdown == "clear"):
        name = "SEQ_SCAN"
    
    for sel, card, p in product(selectivity, cardinality, projections):
        args.qid=f"Filter_ltype{lineage_type}g{sel}card{card}p{p}"
        tname = setup_filter(con, "t1", lineage_type, sel, card, p, folder)
        
        for r in range(args.r):
            q, output_size, avg, mem = core_filter(con, args)
            stats = getStatsWrap(con, q, args, mem)
            plan_timings = parse_plan_timings(args.qid)
            results.append(fill_results(r=r, query=name, runtime=avg, n1=card, n2=None, sel=sel, skew=None, ncol=p, groups=None,
                    index_scan=None, output=output_size, stats=stats, lineage_type=lineage_type, notes=args.notes, plan_timings=plan_timings))

            if args.enable_lineage:
                DropLineageTables(con)
        con.execute("drop table t1")
    con.execute("PRAGMA set_filter='clear'")

def core_aggs(args, con):
    q = "SELECT z, count(*) as agg FROM zipf1 GROUP BY z"

    table_name, method = None, ''
    if args.perm and args.group_concat:
        q = "SELECT z, count(*) as c, group_concat(rowid,',') FROM zipf1 GROUP BY z"
        method="_group_concat"
    elif args.perm and args.list:
        q = "SELECT z, count(*) as c , list(rowid) FROM zipf1 GROUP BY z"
        method="_list"
    elif args.perm and args.window:
        q = "SELECT rowid as rid, z, count(*) over (partition by z) as c FROM zipf1"
        method="_window"
    elif args.perm:
        q = "SELECT zipf1.rowid as rid, z, c FROM (SELECT z, count(*) as c FROM zipf1 GROUP BY z) join zipf1 using (z)"

    q = "create table zipf1_perm_lineage as "+ q
    table_name='zipf1_perm_lineage'
    avg, df, mem = Run(q, args, con, table_name)
    df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
    output_size = df.loc[0,'c']
    con.execute("drop table zipf1_perm_lineage")

    return q, output_size, avg, method, mem

################### int Hash Aggregate  ############
##  Group by: 'z' 
## vary: 'g' number of unique values, cardinality, and skew (TODO)
########################################################
def int_hashAgg(con, args, folder, lineage_type, groups, cardinality, results, agg_type):
    print("------------ Test Int Group By zipfan 1, ", lineage_type, agg_type)
    
    # force operators
    if args.window == False and agg_type == "PERFECT_HASH_GROUP_BY":
        op_code = "perfect"
        con.execute("PRAGMA set_agg='{}'".format(op_code))
    elif args.window == False and agg_type == "HASH_GROUP_BY":
        op_code = "reg"
        con.execute("PRAGMA set_agg='{}'".format(op_code))

    p  = 2
    for g, card in product(groups, cardinality):
        args.qid='gb_ltype{}g{}card{}p{}'.format(lineage_type,g, card, p)

        filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
        zipf1 = pd.read_csv(folder+filename)
        con.register('zipf1_view', zipf1)
        con.execute("create table zipf1 as select * from zipf1_view")

        for r in range(args.r):
            print(r, filename, g, card)
            q, output_size, avg, method, mem = core_aggs(args, con)
            stats = getStatsWrap(con, q, args, mem)
            plan_timings = parse_plan_timings(args.qid)
            results.append(fill_results(r=r, query=agg_type, runtime=avg, n1=card, n2=None, sel=None, skew=None, ncol=p, groups=g,
                    index_scan=None, output=output_size, stats=stats, lineage_type=lineage_type+method, notes=args.notes, plan_timings=plan_timings))
            if args.enable_lineage:
                DropLineageTables(con)
        con.execute("drop table zipf1")
    con.execute("PRAGMA set_agg='clear'")

################### Hash Aggregate  ############
##  Group by on 'z' with 'g' unique values and table size
#   of 'card'. Test on various 'g' values.
########################################################
def hashAgg(con, args, folder, lineage_type, groups, cardinality, results):
    print("------------ Test Group By zipfan 1, ", lineage_type)
    p = 2
    for g, card in product(groups, cardinality):
        args.qid='gb_ltype{}g{}card{}p{}'.format(lineage_type,g, card, p)
        filename = "zipfan_g"+str(g)+"_card"+str(card)+"_a1.csv"
        zipf1 = pd.read_csv(folder+filename)
        zipf1 = zipf1.astype({"z": str})

        con.register('zipf1_view', zipf1)
        con.execute("create table zipf1 as select * from zipf1_view")
        for r in range(args.r):
            print(r, filename, g, card)
            q = "SELECT z, count(*) FROM zipf1 GROUP BY z"
            table_name, method = None, ''
            if args.perm and args.group_concat:
                q = "SELECT z, count(*), group_concat(rowid,',') FROM zipf1 GROUP BY z"
                method="_group_concat"
            elif args.perm and args.list:
                q = "SELECT z, count(*), list(rowid) FROM zipf1 GROUP BY z"
                method="_list"
            elif args.perm:
                q = "SELECT zipf1.rowid, z FROM (SELECT z, count(*) FROM zipf1 GROUP BY z) join zipf1 using (z)"
            q = "create table zipf1_perm_lineage as "+ q
            table_name='zipf1_perm_lineage'
            avg, df, mem = Run(q, args, con, table_name)
            df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
            output_size = df.loc[0,'c']
            con.execute("drop table zipf1_perm_lineage")
            stats = getStatsWrap(con, q, args, mem)
            plan_timings = parse_plan_timings(args.qid)
            results.append(fill_results(r=r, query="HASH_GROUP_BY", runtime=avg, n1=card, n2=None, sel=None, skew=None, ncol=p, groups=g,
                    index_scan=None, output=output_size, stats=stats, lineage_type=lineage_type+method, notes=args.notes, plan_timings=plan_timings))
            if args.enable_lineage:
                DropLineageTables(con)
        con.execute("drop table zipf1")

def setup_join_lessthan(con, card, sel, p):
    """
    t1(v, id, col1, .., colp)
    t2(v, id, col1, .., colp)
    
    len(t1) = card[0]
    len(t2) = card[1]

    t1 JOIN t2 ON (v) --> selectivity of the join condition = sel

    TODO: divide selectivity failures between the two tables
    add values to t1 that fails t1.v < t2.v --> t1.v > t2.v
    add values to t2 that fails t1.v < t2.v --> t2.v < t1.v
    
    n1[fail, succuess ] < n2[fail, success]
    n1[fail] = [10n1 .. 20n1]
    n1[success] = 2n1

    n2[fail] = [0..n1]
    n2[success] = 3n1
    """
    print("sel: ", sel, "card", card, "p", p)
    # create tables & insert values
    n1 = card[0]
    n2 = card[1]
    expected_output = sel * n1 * sel * n2
    k1 = (sel * n1) # seccuss
    k2 = (sel * n2) # success
    print(f"Matching rows for {sel} * {n1} * {sel} * {n2} = {expected_output} {k1} {k2}")
    ## Table 1, len(v1) = n1, random values
    ## that would fail the join condition
    v1 = np.random.uniform(10*n1, 20*n1, n1)
    # pick k random indexes between 0 and n1
    # replace them with values that would eval to true
    IDX = random.sample(range(n1), int(k1))
    v1[IDX] = 2*n1

    print("done generating data v1")
    ## Table 2, len(v2) = n2, random values
    ## that would fail the join condition
    v2 = np.random.uniform(0, n1, n2)
    # pick k random indexes between 0 and n2
    # replace them with values that would eval to true
    IDX = random.sample(range(n2), int(k2))
    v2[IDX] = 3*n1
    print("done generating data v2")
    
    idx1 = list(range(0, n1))
    idx2 = list(range(0, n2))
    t1 = pd.DataFrame({'v':v1, 'id':idx1})
    t2 = pd.DataFrame({'v':v2, 'id':idx2})
    for col in range(p):
        t1["col{}".format(col)]  = np.random.randint(0, 100, len(t1))
        t2["col{}".format(col)]  = np.random.randint(0, 100, len(t2))
    con.register('t1_view', t1)
    con.execute("create table t1 as select * from t1_view")
    con.register('t2_view', t2)
    con.execute("create table t2 as select * from t2_view")
    print("done generating data")

def core_join_less(con, args, pred):
    # Run query
    perm_rid = ''
    if args.perm:
        perm_rid = "t1.rowid as r1_rowid, t2.rowid as t2_rowid, "
    q = "select {}* from t1, t2{}".format(perm_rid, pred)
    table_name = None
    q = "create table zipf1_perm_lineage as "+ q
    table_name='zipf1_perm_lineage'
    avg, df, mem = Run(q, args, con, table_name)
    df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
    output_size = df.loc[0,'c']
    con.execute("drop table zipf1_perm_lineage")
    return q, output_size, avg, mem

################### Joins ###########################
def join_lessthan(con, args, folder, lineage_type, cardinality, results, op, force_join, pred, sels=[0.0]):
    print("------------ Test Join  ", op, pred, force_join)
    if (force_join):
        op_code = "nl"
        if op == "PIECEWISE_MERGE_JOIN":
            op_code = "merge"
        elif op == "BLOCKWISE_NL_JOIN":
            op_code = "bnl"
        elif op == "CROSS_PRODUCT":
            op_code = "cross"
        elif op == "NESTED_LOOP_JOIN":
            op_code = "nl"
        else:
            print("ERROR ", op)
        con.execute("PRAGMA set_join='{}'".format(op_code))
    projections = [0] 
    for sel, card, p in product(sels, cardinality, projections):
        
        setup_join_lessthan(con, card, sel, p)

        for r in range(args.r):
            args.qid='ineq_ltype{}g{}card{}p{}'.format(lineage_type,sel, card, p)
            
            q, output_size, avg, mem = core_join_less(con, args, pred)
            print(avg, output_size)

            stats = getStatsWrap(con, q, args, mem)
            plan_timings = parse_plan_timings(args.qid)
            results.append(fill_results(r=r, query=op, runtime=avg, n1=card[0], n2=card[1], sel=sel, skew=None, ncol=p, groups=None,
                    index_scan=None, output=output_size, stats=stats, lineage_type=lineage_type, notes=args.notes, plan_timings=plan_timings))
            if args.enable_lineage:
                DropLineageTables(con)
        con.execute("drop table t1")
        con.execute("drop table t2")
    con.execute("PRAGMA set_join='clear'")

def setup_pt(con, g, op):
    # PT(id int, v int)
    # cardinality = g
    idx = list(range(0, g))
    vals = np.random.uniform(0, 100, g)
    PT = pd.DataFrame({'id':idx, 'v':vals})
    con.register('PT_view', PT)
    con.execute("create table PT as select * from PT_view")
    if (op == "INDEX_JOIN"):
        con.execute("create index i_index ON PT using art(id);");

def setup_ft(con, g, card, a, op, folder):
    fname =  f"zipfan_g{g}_card{card}_a{a}.csv"
    FT = pd.read_csv(folder+fname)
    con.register('FT_view', FT)
    con.execute("create table FT as select * from FT_view")

def core_pkfk(con, args, op, index_scan):
    perm_rid = ''
    if args.perm:
        perm_rid = "FT.rowid as FT_rowid, PT.rowid as PT_rowid,"
    if (op == "INDEX_JOIN"):
        if (index_scan):
            q = "SELECT {}FT.z, FT.v FROM PT, FT WHERE PT.id=FT.z".format(perm_rid)
        else:
            q = "SELECT {}PT.id, FT.v FROM PT, FT WHERE PT.id=FT.z".format(perm_rid)
    else:
        q = "SELECT {}FT.v, PT.id FROM FT, PT WHERE PT.id=FT.z".format(perm_rid)
    table_name = None
    q = "create table perm_lineage as "+ q
    table_name='perm_lineage'
    avg, df, mem = Run(q, args, con, table_name)
    df = con.execute("select count(*) as c from perm_lineage").fetchdf()
    output_size = df.loc[0,'c']
    con.execute("drop table perm_lineage")

    return q, output_size, avg, mem

def FKPK(con, args, folder, lineage_type, groups, cardinality, a_list, results, op, index_scan):
    # SELECT * FROM gids,zipf WHERE gids.id=zipf.z. zipf.z 
    # zipf.z is a foreign key that references gids.id and 
    # is drawn from a zipfian distribution (θ = 1) 
    # We vary the number of join matches by varying the unique values for gids.id
    # g = (100, 10000)
    # n = 1M, 5M, 10M
    print("------------ Test FK-PK ", op, index_scan)
    if (op == "INDEX_JOIN"):
        con.execute("PRAGMA explain_output = PHYSICAL_ONLY;")
        con.execute("PRAGMA force_index_join")
    p = 2
    for g in groups:
        setup_pt(con, g, op)
        for a, card in product(a_list, cardinality):
            args.qid='ineq_ltype{}g{}card{}p{}'.format(lineage_type,g, card, p, a)
            setup_ft(con, 10000, card, a, op, folder)
            for r in range(args.r):
                q, output_size, avg, mem = core_pkfk(con, args, op, index_scan)
                print(avg, output_size)
                
                stats = getStatsWrap(con, q, args, mem)
                plan_timings = parse_plan_timings(args.qid)
                results.append(fill_results(r=r, query=op, runtime=avg, n1=card, n2=g, sel=None, skew=a, ncol=p, groups=g,
                        index_scan=index_scan, output=output_size, stats=stats, lineage_type=lineage_type, notes=args.notes, plan_timings=plan_timings))
                if args.enable_lineage:
                    DropLineageTables(con)
            con.execute("drop table FT")
        if (op == "INDEX_JOIN"):
            con.execute("DROP INDEX i_index")
        con.execute("drop table PT")

def setup_table(con, g, card, a, build_index, folder, tname):
    fname =  f"zipfan_g{g}_card{card}_a{a}.csv"
    t = pd.read_csv(folder+fname)

    unique_elements, counts = np.unique(t['z'], return_counts=True)
    print(f"g={g}, card={card}, a={a}, len={len(t['z'])}")
    print("1. ", len(unique_elements), unique_elements[:10], unique_elements[len(unique_elements)-10:])
    print("2. ", counts[:10], counts[len(counts)-10:])

    con.register(f'{tname}_view', t)
    con.execute(f"create table {tname} as select * from {tname}_view")
    if (build_index == True):
        con.execute(f"create index i_index ON {tname} using art(z);");

def core_mtn(con, args, op, index_scan):
    perm_rid = ''
    if args.perm:
        perm_rid = "zipf1.rowid as zipf1_rowid, zipf2.rowid as zipf2_rowid,"
    if (op == "INDEX_JOIN"):
        if (index_scan): # avoid accessing the index
            q = "SELECT {}zipf2.idx, zipf2.v FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z".format(perm_rid)
        else:
            q = "SELECT {}zipf2.idx, zipf1.v FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z".format(perm_rid)
    else:
        q = "SELECT {}zipf2.idx, zipf1.v FROM zipf1, zipf2 WHERE zipf1.z=zipf2.z".format(perm_rid)
    table_name = None
    q = "create table perm_lineage as "+ q
    table_name='perm_lineage'
    avg, df, mem = Run(q, args, con, table_name)
    df = con.execute("select count(*) as c from perm_lineage").fetchdf()
    output_size = df.loc[0,'c']
    con.execute("drop table perm_lineage")

    return q, output_size, avg, mem

def MtM(con, args, folder, lineage_type, groups, cardinality, a_list, results, op, index_scan):
    print("------------ Test M:N ", op, index_scan)
    # SELECT * FROM zipf1,zipf2 WHERE zipf1.z=zipf2.z
    # zipfian distributions (θ = 1)
    # zipf1.z is within [1,10] or [1, 100] while zipf2.z∈ [1, 100]
    # This means that tuples with z = 1 have a disproportionately large number of
    # matches compared to larger z values that have fewer matches.
    # For this experiment, we also fix the size of the left table zipf1 to 10^3 records
    # and vary the right zipf2 from 10^3 to 10^5

    build_index = False
    if (op == "INDEX_JOIN"):
        con.execute("PRAGMA explain_output = PHYSICAL_ONLY;")
        con.execute("PRAGMA force_index_join")
        build_index = True
    p = 2
    n1 = 1000
    for a, g, card in product(a_list, groups, cardinality):
        g2 = g
        # g either 10 or 100
        setup_table(con, g, n1, a, build_index, folder, "zipf1")
        args.qid='ineq_ltype{}g{}card{}p{}'.format(lineage_type,g, card, p, a)
        setup_table(con, g2, card, 1, False, folder, "zipf2")
        for r in range(args.r):
            q, output_size, avg, mem = core_mtn(con, args, op, index_scan)
            sel = float(output_size) / (card*n1)
            print("\n", avg, output_size, sel)
            
            stats = getStatsWrap(con, q, args, mem)
            plan_timings = parse_plan_timings(args.qid)
            results.append(fill_results(r=r, query=op+"_mtm", runtime=avg, n1=card, n2=g, sel=sel, skew=a, ncol=p, groups=g,
                    index_scan=index_scan, output=output_size, stats=stats, lineage_type=lineage_type, notes=args.notes, plan_timings=plan_timings))
            if args.enable_lineage:
                DropLineageTables(con)
        con.execute("drop table zipf2")
        if (op == "INDEX_JOIN"):
            con.execute("DROP INDEX i_index")
        con.execute("drop table zipf1")
