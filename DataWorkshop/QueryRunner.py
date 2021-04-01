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
            result = self.sum()
        elif list(self.func[0].keys())[0] == 'avg':
            result = self.avg()
        else:
            result = self.connectors[0].execute(self.func)
            for con in self.connectors[1:]:
                part = con.execute(self.func)
                result = merge(result, part)

        return result

    def sum(self):
        complex = list(self.func[0].keys())[0]
        simple = self.func[0][complex]
        sum_result = {}
        for con in self.connectors:
            part = con.execute(simple)
            for p in part:
                for k in p.keys():
                    sum_result[k] = sum_result.get(k, 0) + p[k]

        return sum_result

    def avg(self):
        complex = list(self.func[0].keys())[0]
        simple = self.func[0][complex]
        elem_number = 0
        sum = {}
        for con in self.connectors:
            part = con.execute(simple)
            elem_number += len(part)
            for p in part:
                for k in p.keys():
                    sum[k] = sum.get(k, 0) + p[k]

        avg = {}

        for key in sum.keys():
            avg[key] = avg.get(key, sum[key]/elem_number)

        return avg


