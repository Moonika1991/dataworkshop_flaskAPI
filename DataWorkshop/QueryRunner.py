from .QueryParser import QueryParser
from .ConnectorFactory.ConnectorFactory import ConnectorFactory
import pandas as pd
from jsonmerge import merge


class QueryRunner():
    def __init__(self, query):
        self.parser = QueryParser(query)
        self.source = self.parser.get_source()
        self.func = self.parser.get_func()
        connector_factory = ConnectorFactory()
        self.connectors = []
        for s in self.source:
            self.connectors.append(connector_factory.create_connector(s))

    def get_source(self):
        return self.source

    def run(self):
        result = None
        if len(self.connectors) > 1:
            for con in self.connectors[1:]:
                part = con.execute(self.func)
                result = merge(result, part)

        return result
