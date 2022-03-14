from pathlib import Path
import os

from sample_workload import sample_workload
from conn_utils import *
from index_recommendation import recommend_index

MAX_SAMPLE_COUNT = 1000

def task_project1_setup():
    return {
        "actions": [
            # Install python psql client, and hypopg.
            './install_deps.sh',
        ]
    }

def task_project1():
    def tune_iteration(workload_csv, db_name, db_user, db_pswd, timeout):
        collected_queries = sample_workload(workload_csv, MAX_SAMPLE_COUNT)
        conn = get_conn('localhost', db_name, db_user, db_pswd)
        # Create hypopg extensions.
        run_query(conn, 'CREATE EXTENSION IF NOT EXISTS hypopg')
        # Get the recommendation of this iteration based on the simplified Dexter and HypoPg.
        index_recommend = recommend_index(collected_queries, conn, timeout)
        with open('actions.sql', 'w') as fw:
            for index_query in index_recommend:
                fw.write(f"{index_query}\n")


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

