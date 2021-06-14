from datetime import datetime
from dags.dynamic_dag_utils import create_dag

default_args = {
    "owner": "airflow",
    "start_date": datetime(2018, 1, 1),
    "email": ["someEmail@gmail.com"],
    "email_on_failure": False,
}

definition = {
    "name": "demo_7",
    "nodes": [
        {
            "name": "Start",
            "_type": "airflow.operators.dummy.DummyOperator",
            "parameters": {},
        },
        {
            "name": "HTTPRequest",
            "_type": "airflow.providers.http.operators.http.SimpleHttpOperator",
            "position": [
                520,
                360
            ],
            "parameters": {
                "http_conn_id": "dummyapi",
                "method": "GET",
                "endpoint": "/api/users/2",
                            "headers": {},
                            "extra_options": {}
            }
        },
        {
            "name": "End",
            "_type": "airflow.operators.dummy.DummyOperator",
            "parameters": {},
        },
        {
            "name": "ExecuteCommand",
            "_type": "airflow.operators.bash.BashOperator",
            "parameters": {
                "bash_command": "echo test"
            },
        }
    ],
    "connections": {
        "Start": ["ExecuteCommand", "HTTPRequest"],
        "HTTPRequest": ["End"],
        "ExecuteCommand": ["End"]
    }
}

dag = create_dag(None, default_args, definition)
