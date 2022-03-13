from pathlib import Path
import os

def task_project1_setup():
    return {
        "actions": [
            # Install python psql client, and hypopg.
            './install_deps.sh',
        ]
    }

def task_project1():
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
    }

