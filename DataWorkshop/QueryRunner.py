from .QueryParser import QueryParser
from .ConnectorFactory.ConnectorFactory import ConnectorFactory


class QueryRunner():
    def __init__(self, query):
        self.parser = QueryParser(query)
        self.source = self.parser.get_source()
        self.connectors = []
        for s in self.source:
            ConnectorFactory().create_connector(s)
        self.func = self.parser.get_func()

    def get_source(self):
        return self.source

    def run(self):
        result = []
        for con in self.connectors:
            result = result.append(con.execute(self.func))
        return result
