from abc import ABC
from pathlib import Path
import json
from .Connector import Connector
from .SQLConnector import SQLConnector
from .CSVConnector import CSVConnector


class ConnectorFactory(ABC):

    def create_connector(self, filename) -> Connector:
        # finding path by given filename
        path = Path(__file__).resolve().parents[2]
        for name in filename:
            connector = json.load(open("%s\\etc\\connectors\\%s.json" % (path, name)))
            targetclass = connector.get('type').upper() + 'Connector'

        return  globals()[targetclass](connector)

