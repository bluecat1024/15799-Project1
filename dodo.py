from pathlib import Path
import os
import time

os.system("sudo apt-get -y install python3-dev libpq-dev")
os.system("pip3 install psycopg2")

from sample_workload import sample_workload
from conn_utils import *
from index_recommendation import recommend_index, drop_index

MAX_SAMPLE_COUNT = 1000

def convert_time(timeout):
    if timeout[-1].lower() == 's':
        return int(timeout[:-1])
    elif timeout[-1].lower() == 'm':
        return 60 * int(timeout[:-1])
    elif timeout[-1].lower() == 'h':
        return 3600 * int(timeout[:-1])
    else:
        return int(timeout)

def task_project1_setup():
    return {
        "actions": [
            # Install python psql client, and hypopg.
            './install_deps.sh',
        ]
    }

def task_project1():
    def tune_iteration(workload_csv, db_name, db_user, db_pswd, timeout):
        # Convert time to correct format.
        start_ts = time.time()
        timeout = convert_time(timeout)

        # Collect workload trace.
        collected_queries = sample_workload(workload_csv, MAX_SAMPLE_COUNT)
        conn = get_conn('localhost', db_name, db_user, db_pswd)
        # Create hypopg extensions.
        run_query(conn, 'CREATE EXTENSION IF NOT EXISTS hypopg')
        # Get the recommendation of this iteration based on the simplified Dexter and HypoPg.
        add_index_list = []
        hypo_added_index = set()
        while True:
            # If tuning time is quite long, do not continue to add indexes.
            if time.time() - start_ts > 0.7 * timeout:
                break
            index_recommend = recommend_index(collected_queries, conn, hypo_added_index)
            add_index_list += index_recommend
            # Also stops if no more to add for current trace.
            if len(index_recommend) == 0:
                break

        # Continue to drop useless index.
        drop_index_list = []
        hypo_dropped_index = set()
        # Only try to drop indexes when no more to add in this iteration.
        while len(add_index_list) == 0:
            # If tuning time is quite long, do not continue to drop indexes.
            if time.time() - start_ts > 0.7 * timeout:
                break
            drop_recommend = drop_index(collected_queries, conn, hypo_dropped_index)
            drop_index_list += drop_recommend
            # Also stops if no more to drop for current trace.
            if len(drop_recommend) == 0:
                break

        # Remove any hypothetical configurations at the end.
        run_query(conn, 'select * from hypopg_reset()')
        conn.commit()
        with open('actions.sql', 'w') as fw:
            for index_query in add_index_list:
                fw.write(f"{index_query}\n")
            for drop_query in drop_index_list:
                fw.write(f"{drop_query}\n")


    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            'echo "Faking action generation."',
            tune_iteration,
            'echo \'{"VACUUM": false}\' > config.json',
        ],
        # Always rerun this task.
        "uptodate": [False],
        # Parameters (Supported and Unsupported)
        "params": [
            {
                "name": "workload_csv",
                "long": "workload_csv",
                "help": "path to raw csv",
                "default": None,
            },
            {
                "name": "db_name",
                "long": "db_name",
                "help": "target dbms name",
                "default": "project1db",
            },
            {
                "name": "db_user",
                "long": "db_user",
                "help": "target dbms user",
                "default": "project1user",
            },
            {
                "name": "db_pswd",
                "long": "db_pswd",
                "help": "target dbms password",
                "default": "project1pass",
            },
            {
                "name": "timeout",
                "long": "timeout",
                "help": "timeout given by grader",
                "default": "10m",
            },
        ],
    }

