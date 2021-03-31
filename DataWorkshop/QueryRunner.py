from .QueryParser import QueryParser
from .ConnectorFactory.ConnectorFactory import ConnectorFactory
import pandas as pd
from jsonmerge import merge


class QueryRunner():
    def __init__(self, query):
        self.parser = QueryParser(query)
        self.source = self.parser.get_source()
        self.func = self.parser.get_func()
        self.connector_factory = ConnectorFactory()
        self.connectors = []
        for s in self.source:
            self.connectors.append(self.connector_factory.create_connector(s))

    def get_source(self):
        return self.source

    def run(self):
        result = None
        # dealing with complex functions
        if list(self.func[0].keys())[0] == 'sum':
            sum = 0
            complex = list(self.func[0].keys())[0]
            simple = self.func[0][complex]
            for con in self.connectors:
                part = con.execute(simple)
                for dic in part:
                    for key in dic:
                        sum += dic[key]
            result = sum

        elif len(self.connectors) > 1:
            result = self.connectors[0].execute(self.func)
            for con in self.connectors[1:]:
                part = con.execute(self.func)
                result = merge(result, part)

        return result

