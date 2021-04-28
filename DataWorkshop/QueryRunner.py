from .QueryParser import QueryParser
from .ConnectorFactory.ConnectorFactory import ConnectorFactory
import pandas as pd
from jsonmerge import merge
from datetime import datetime
import collections


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

        # sort function isn't necessary
        # elif list(self.func[0].keys())[0] == 'sort':
        # result = self.sort()

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
        res_list = []
        for con in self.connectors:
            part = con.execute(simple)
            for p in part:
                for k in p.keys():
                    sum_result[k] = sum_result.get(k, 0) + p[k]

        # result must be a list of dicts
        res_list.append(sum_result)

        return res_list

    def avg(self):
        complex = list(self.func[0].keys())[0]
        simple = self.func[0][complex]
        elem_number = 0
        sum = {}
        res_list = []
        for con in self.connectors:
            part = con.execute(simple)
            elem_number += len(part)
            for p in part:
                for k in p.keys():
                    sum[k] = sum.get(k, 0) + p[k]

        avg = {}

        for key in sum.keys():
            avg[key] = avg.get(key, sum[key] / elem_number)

        # result must be a list of dicts
        res_list.append(avg)

        return res_list

    '''
    sort function isn't needed
    def sort(self):
        complex = list(self.func[0].keys())[0]
        sort_args = self.func[0][complex]
        # function must be a list oif dicts
        simple = [sort_args[0]]
        # format of keys to sort
        dt_format = sort_args[1]
        res = self.connectors[0].execute(simple)
        for con in self.connectors[1:]:
            part = con.execute(simple)
            res = merge(res, part)

        res = res[0]
        if dt_format == 'num':
            sort_res = collections.OrderedDict(sorted(res.items()))
        else:
            # list of acceptable date/time formats
            for ch in ['a', 'A', 'B', 'w', 'm', 'p', 'y', 'd']:
                if ch in dt_format:
                    dt_format = dt_format.replace(ch, "%"+ch)
            sort_res = collections.OrderedDict(sorted(res.items(), key=lambda x: datetime.strptime(x[0], dt_format)))

        return [dict(sort_res)]
    '''
