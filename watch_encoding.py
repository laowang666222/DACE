from utils import load_pickle
from plan_utils import read_workload_runs , get_plan_encoding
import os
from utils import workloads
from utils import load_json,ROOT_DIR,plan_parameters
import json
from plan_utils import add_numerical_scalers, get_op_name_to_one_hot

def watch_encoding():
    pre_process_path="data/workload1/plans_meta.pkl"
    plans_meta = load_pickle(pre_process_path)

    for plan_meta in plans_meta:
        print(plan_meta)
        break
    
def watch_first_plan():
    plans = read_workload_runs(
        os.path.join("data/workload1"), db_names=workloads, verbose=True
    )
    plan = plans[0]
    print(plan)

def watch_imdb_full():
    plans = load_json("data/workload1/imdb_full.json")
    print_count = 0
    for plan in plans:
        plan_str = json.dumps(plan)
        if "Nested Loop" in plan_str and "char_name" in plan_str:
            print_count += 1
            print(plan_str)
            if print_count > 3:
                break

def watch_nestloop_encoding():
    plan_str = '''{"Plan": {"Node Type": "Aggregate", "Strategy": "Plain", "Partial Mode": "Simple", "Parallel Aware": false, "Async Capable": false, "Startup Cost": 897387.04, "Total Cost": 897387.05, "Plan Rows": 1, "Plan Width": 8, "Actual Startup Time": 15993.975, "Actual Total Time": 15993.981, "Actual Rows": 1, "Actual Loops": 1, "Plans": [{"Node Type": "Nested Loop", "Parent Relationship": "Outer", "Parallel Aware": false, "Async Capable": false, "Join Type": "Left", "Startup Cost": 39700.36, "Total Cost": 896669.37, "Plan Rows": 95689, "Plan Width": 12, "Actual Startup Time": 367.666, "Actual Total Time": 15990.256, "Actual Rows": 24234, "Actual Loops": 1, "Inner Unique": true, "Plans": [{"Node Type": "Hash Join", "Parent Relationship": "Outer", "Parallel Aware": false, "Async Capable": false, "Join Type": "Inner", "Startup Cost": 39699.93, "Total Cost": 783747.15, "Plan Rows": 95689, "Plan Width": 12, "Actual Startup Time": 367.159, "Actual Total Time": 14301.996, "Actual Rows": 24234, "Actual Loops": 1, "Inner Unique": true, "Hash Cond": "(cast_info.person_id = aka_name.id)", "Plans": [{"Node Type": "Nested Loop", "Parent Relationship": "Outer", "Parallel Aware": false, "Async Capable": false, "Join Type": "Inner", "Startup Cost": 0.44, "Total Cost": 739340.39, "Plan Rows": 95719, "Plan Width": 16, "Actual Startup Time": 0.276, "Actual Total Time": 13833.791, "Actual Rows": 63131, "Actual Loops": 1, "Inner Unique": true, "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Parallel Aware": false, "Async Capable": false, "Relation Name": "cast_info", "Alias": "cast_info", "Startup Cost": 0.0, "Total Cost": 705710.9, "Plan Rows": 245707, "Plan Width": 20, "Actual Startup Time": 0.051, "Actual Total Time": 3920.66, "Actual Rows": 258228, "Actual Loops": 1, "Filter": "((note)::text ~~ '%footage)%'::text)", "Rows Removed by Filter": 35986116}, {"Node Type": "Memoize", "Parent Relationship": "Inner", "Parallel Aware": false, "Async Capable": false, "Startup Cost": 0.44, "Total Cost": 1.24, "Plan Rows": 1, "Plan Width": 4, "Actual Startup Time": 0.038, "Actual Total Time": 0.038, "Actual Rows": 0, "Actual Loops": 258228, "Cache Key": "cast_info.person_role_id", "Cache Mode": "logical", "Cache Hits": 215098, "Cache Misses": 43130, "Cache Evictions": 0, "Cache Overflows": 0, "Peak Memory Usage": 4028, "Plans": [{"Node Type": "Index Scan", "Parent Relationship": "Outer", "Parallel Aware": false, "Async Capable": false, "Scan Direction": "Forward", "Index Name": "char_name_pkey", "Relation Name": "char_name", "Alias": "char_name", "Startup Cost": 0.43, "Total Cost": 1.23, "Plan Rows": 1, "Plan Width": 4, "Actual Startup Time": 0.227, "Actual Total Time": 0.227, "Actual Rows": 1, "Actual Loops": 43130, "Index Cond": "(id = cast_info.person_role_id)", "Rows Removed by Index Recheck": 0, "Filter": "((surname_pcode)::text !~~ '%M%5%'::text)", "Rows Removed by Filter": 0}]}]}, {"Node Type": "Hash", "Parent Relationship": "Inner", "Parallel Aware": false, "Async Capable": false, "Startup Cost": 24916.14, "Total Cost": 24916.14, "Plan Rows": 901068, "Plan Width": 8, "Actual Startup Time": 364.069, "Actual Total Time": 364.071, "Actual Rows": 901128, "Actual Loops": 1, "Hash Buckets": 131072, "Original Hash Buckets": 131072, "Hash Batches": 16, "Original Hash Batches": 16, "Peak Memory Usage": 3234, "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Parallel Aware": false, "Async Capable": false, "Relation Name": "aka_name", "Alias": "aka_name", "Startup Cost": 0.0, "Total Cost": 24916.14, "Plan Rows": 901068, "Plan Width": 8, "Actual Startup Time": 0.01, "Actual Total Time": 215.34, "Actual Rows": 901128, "Actual Loops": 1, "Filter": "(((name)::text !~~ '%John%'::text) OR ((name)::text !~~ '%David%'::text))", "Rows Removed by Filter": 215}]}]}, {"Node Type": "Index Scan", "Parent Relationship": "Inner", "Parallel Aware": false, "Async Capable": false, "Scan Direction": "Forward", "Index Name": "title_pkey", "Relation Name": "title", "Alias": "title", "Startup Cost": 0.43, "Total Cost": 1.18, "Plan Rows": 1, "Plan Width": 8, "Actual Startup Time": 0.069, "Actual Total Time": 0.069, "Actual Rows": 1, "Actual Loops": 24234, "Index Cond": "(id = cast_info.movie_id)", "Rows Removed by Index Recheck": 0}]}]}, "Planning Time": 2.747, "Triggers": [], "Execution Time": 15995.653}'''
    plan = json.loads(plan_str)
    plan['database_id'] = 13

    configs = load_json("configs/imdb_full.json")
    statistics_file_path = configs["statistics_path"]

    feature_statistics = load_json(ROOT_DIR + statistics_file_path)
    # add numerical scalers (cite from zero-shot)
    add_numerical_scalers(feature_statistics)

    # op_name to one-hot, using feature_statistics
    op_name_to_one_hot = get_op_name_to_one_hot(feature_statistics)
    plan_mata = get_plan_encoding(
            plan, configs, op_name_to_one_hot, plan_parameters, feature_statistics
        )
    times = plan_mata[1]
    print((times-1e-7)*30)
    print(plan_mata)

if __name__ == "__main__":
    # watch_imdb_full()
    watch_nestloop_encoding()
