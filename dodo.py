from pathlib import Path
import os

from sample_workload import sample_workload

MAX_SAMPLE_COUNT = 1000

def task_project1_setup():
    return {
        "actions": [
            # Install python psql client, and hypopg.
            './install_deps.sh',
        ]
    }

def task_project1():
    def tune_iteration(workload_csv, db_name, db_user, db_pass):
        sample_workload(workload_csv, MAX_SAMPLE_COUNT)

    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            'echo "Faking action generation."',
            'echo "SELECT 1;" > actions.sql',
            'echo "SELECT 2;" >> actions.sql',
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

