import json
from airflow.models.connection import Connection
from typing import Dict


class ConnectionGrabber:
    def __getattr__(self, name):
        return Connection.get_connection_from_secrets(name)


def from_json(text: str) -> Dict:
    return json.loads(text)
