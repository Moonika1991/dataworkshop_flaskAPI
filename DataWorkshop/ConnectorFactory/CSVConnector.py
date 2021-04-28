from .Connector import Connector
import pandas as pd
import copy


class CSVConnector(Connector):

    def __init__(self, access):
        self._start_object = pd.read_csv(access.get('path'), sep=access.get('sep'))

    def execute(self, query):
        # copy.deepcopy()- necessary to work on copy not a real query
        res = self.exec_recurent(copy.deepcopy(query))
        return res.to_dict('records')

    def exec_recurent(self, query):
        fun = list(query[0].keys())[0]
        args = query[0][fun]
        res_args = args
        part_result = query
        result = pd.DataFrame()
        if any(type(arg) is dict for arg in args):
            pos = 0
            for arg in args:
                if type(arg) is str:
                    pos += 1
                    continue
                # query must be a list of dicts (arg is dict)
                temp_query = [arg]
                part = self.exec_recurent(temp_query)
                if type(arg) is pd.DataFrame:
                    pos += 1
                    continue
                else:
                    res_args.pop(pos)
                    res_args.insert(pos, part)
                    pos += 1
            part_result[0][fun] = res_args
            result = self.exec_recurent(part_result)
        elif fun == 'col':
            result = self.col(args)
        elif all(type(arg) is str for arg in args):
            col = args[0]
            if args[1].isdigit():
                val = float(args[1])
            else:
                val = args[1]
            if fun == 'equal':
                result = self._start_object.loc[self._start_object[col] == val]
            elif fun == 'gt':
                result = self._start_object.loc[self._start_object[col] > val]
            elif fun == 'lt':
                result = self._start_object.loc[self._start_object[col] < val]
            elif fun == 'goe':
                result = self._start_object.loc[self._start_object[col] >= val]
            elif fun == 'loe':
                result = self._start_object.loc[self._start_object[col] <= val]
        elif fun == 'or':
            result = self.alt(args)
        elif fun == 'and':
            result = self.conj(args)
        elif fun == 'exc':
            result = self.exc(args)
        return result

    def alt(self, args):
        result = pd.DataFrame()
        for arg in args:
            result = pd.concat([result, arg]).drop_duplicates()
            result = result.sort_index()
        return result

    def conj(self, args):
        comp = args[0]
        result = pd.DataFrame()
        for arg in args[1:]:
            comp = comp.merge(arg, indicator=True, how='outer')
            result = comp[comp['_merge'] == 'both']
            result = result.drop('_merge', 1)
        return result

    def col(self, args):
        result = pd.DataFrame()
        if all(type(arg) is str for arg in args):
            df = self._start_object
            col = df[args]
            # concat axis=1/'columns'
            result = pd.concat([result, col], 1)
        else:
            df = args[0]
            for arg in args[1:]:
                col = df[arg]
                # concat axis=1/'columns'
                result = pd.concat([result, col], 1)
        return result

    def exc(self, args):
        result = args[0]
        for arg in args[1:]:
            result = result.drop(arg, 1)
        return result
