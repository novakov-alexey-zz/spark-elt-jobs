import json
import os

from datetime import datetime
from dags.dynamic_dag_utils import create_dag

default_args = {
    "owner": "alexey",
    "start_date": datetime(2021, 1, 1),
    "email": ["someEmail@gmail.com"],
    "email_on_failure": False,
}

DAGS_FOLDER = "{{fromjson(connection.dags_folder.extra)['path']}}"

# path to JSON file must be inserted during this python file generation
json_location = os.path.join(DAGS_FOLDER, "json/demo_dag.json")

with open(json_location) as file:
    definition = json.load(file)
    dag_id, dag = create_dag(None, default_args, definition)
    globals()[dag_id] = dag
