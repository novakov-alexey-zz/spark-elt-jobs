from importlib import import_module
from airflow import DAG


def create_dag(schedule, default_args, definition):
    """Create dags dynamically."""
    with DAG(
        definition["name"], schedule_interval=schedule, default_args=default_args
    ) as dag:

        tasks = {}
        for node in definition["nodes"]:
            operator = load_operator(node["_type"])
            params = node["parameters"]

            node_name = node["name"].replace(" ", "")
            params["task_id"] = node_name
            params["dag"] = dag
            tasks[node_name] = operator(**params)

        for node_name, downstream_conn in definition["connections"].items():
            for ds_task in downstream_conn:
                tasks[node_name] >> tasks[ds_task]

    globals()[definition["name"]] = dag
    return dag


def load_operator(name):
    """Load operators dynamically"""
    components = name.rpartition('.')
    return getattr(import_module(components[0]), components[-1])
