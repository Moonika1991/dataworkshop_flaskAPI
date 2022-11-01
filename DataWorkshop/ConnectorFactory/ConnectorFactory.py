from abc import ABC
from pathlib import Path
import json
from .Connector import Connector
from .SQLConnector import SQLConnector
from .CSVConnector import CSVConnector


class ConnectorFactory(ABC):

    def create_connector (self, filename) -> Connector:
        # finding path by given filename
        path = Path(__file__).resolve().parents[2]
        connector = json.load(open("%s\\etc\\connectors\\%s.json" % (path, filename)))
        targetclass = connector.get('type').upper() + 'Connector'

        # creates concrete connector
        return globals()[targetclass](connector)

