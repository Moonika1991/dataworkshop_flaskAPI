from .QueryParser import QueryParser
from .ConnectorFactory.ConnectorFactory import ConnectorFactory
import pandas as pd
from datetime import datetime
import collections
import copy
from pathlib import Path
import json


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
        # sort function isn't necessary
        # elif list(self.func[0].keys())[0] == 'sort':
        # result = self.sort()
        result = self.connectors[0].execute(self.func)
        for con in self.connectors[1:]:
            part = con.execute(self.func)
            result = result + part

        if len(self.func) > 1:  # if complex functions are in query
            for func in self.func[1:]:
                if list(func.keys())[0] == 'agg':
                    agg_args = func['agg']
                    result = self.agg(result, agg_args)
                elif list(func.keys())[0] == 'pivot':
                    pivot_args = func['pivot']
                    result = self.pivot(result, pivot_args)
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

    def agg(self, dict_list, arg_list):
        res_tab = []
        keys_list = []  # list of keys for aggregate function
        func_list = [] # list of functions inside aggregate
        # add keys and functions to proper lists
        for elem in arg_list:
            if type(elem) is str:
                keys_list.append(elem)
            else:
                func_list.append(elem)
        # create a list of lists containing  dictionaries where values for aggregate keys are equal
        for dict in dict_list:
            same_list = []
            for dic in dict_list:
                flag = True
                for key in keys_list:
                    if dict[key] != dic[key]:
                        flag = False
                if flag:
                    same_list.append(dic)
            res_tab.append(same_list)  # list of lists containing dicts where values for aggregate keys are equal
        result = []
        for func in func_list:
            if list(func.keys())[0] == 'sum':
                # for every dict in every list of dicts sum values for sum key
                
                for tab in res_tab:
                    sum_res = 0
                    for t in tab:
                        sum_res = sum_res + t[func['sum'][0]]
                    dict_res = {}
                    for key in keys_list:
                        # can't work on actual tab, because it'll destroy the loop
                        dict_res[key] = copy.deepcopy(tab[0][key])
                    dict_res['sum'] = sum_res  # add sum to result dictionary
                    if dict_res not in result:  # to avoid duplicates
                        result.append(dict_res)
            elif list(func.keys())[0] == 'avg':
                for tab in res_tab:
                    sum_res = 0
                    i = 0
                    for t in tab:
                        sum_res = sum_res + t[func['avg'][0]]
                        i = i +1
                    avg = sum_res/i
                    dict_res = {}
                    for key in keys_list:
                        dict_res[key] = copy.deepcopy(tab[0][key])
                    dict_res['avg'] = avg
                    if dict_res not in result:
                        result.append(dict_res)
        return result

    def pivot(self, dict_list, args):
        result = []
        for dict in dict_list:
            # res_dict = {row_name : value of this key from dict, value of dict key for column key from query :
            res_dict = {args[1]: dict[args[1]], dict[args[0]]: dict[args[2]]}
            for dic in dict_list:
                if dict[args[1]] == dic[args[1]]:
                    if dic[args[0]] not in res_dict.keys():
                        res_dict[dic[args[0]]] = dic[args[2]]
            if res_dict not in result:
                result.append(res_dict)

        # to fill missing keys
        for dict in result:
            for dic in result:
                for key in dic.keys():
                    tmp = {}
                    if key not in dict.keys():
                        dict[key] = 0

        result = self.sort_keys(result)
            #  dict = sorted(dict.items(), key=lambda x: datetime.strptime(x[0], '%d.%m.%Y'), reverse=False)
        return result

    def sort_keys(self, dict_list):
        res = []
        for dic in dict_list:
            temp = {}
            key = list(dic.keys())[0]
            value = dic[key]
            temp[key] = value  # save dict key and value in temp
            dic.pop(key)
            if self.validate_date(dic):
                dic = sorted(dic.items(), key=lambda x: datetime.strptime(x[0], '%Y.%m.%d'), reverse=False)
            else:
                dic = sorted(dic.items())
            dic = dict(dic)
            temp.update(dic)
            res.append(temp)

        return res

    def validate_date(self, dict):
        try:
            datetime.strptime(list(dict.keys())[0], '%Y.%m.%d')
            flag = True
        except ValueError:
            flag = False

        return flag

    '''
    sort function isn't needed problem solved with app.config['JSON_SORT_KEYS'] = False
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