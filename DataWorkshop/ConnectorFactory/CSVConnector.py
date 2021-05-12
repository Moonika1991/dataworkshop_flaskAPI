from .Connector import Connector
import pandas as pd
import copy


class CSVConnector(Connector):

    def __init__(self, access):
        self._start_object = pd.read_csv(access.get('path'), sep=access.get('sep'))

    def execute(self, query):
        # copy.deepcopy()- necessary to work on copy not a real query
        # real query can be necessary for other connectors
        res = self.exec_recurrent(copy.deepcopy(query))
        return res.to_dict('records')

    def equal(self, args):
        col = args[0]
        if args[1].isdigit():
            val = float(args[1])
        else:
            val = args[1]
        result = self._start_object.loc[self._start_object[col] == val]
        return result

    def gt(self, args):
        col = args[0]
        val = float(args[1])
        result = self._start_object.loc[self._start_object[col] > val]
        return result

    def lt(self, args):
        col = args[0]
        val = float(args[1])
        result = self._start_object.loc[self._start_object[col] < val]
        return result

    def goe(self, args):
        col = args[0]
        val = float(args[1])
        result = self._start_object.loc[self._start_object[col] >= val]
        return result

    def loe(self, args):
        col = args[0]
        val = float(args[1])
        result = self._start_object.loc[self._start_object[col] <= val]
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
        if all(type(arg) is str for arg in args):
            result = self._start_object
            for arg in args:
                # axis = 1 to drop column not index
                result = result.drop(arg, 1)
        else:
            result = args[0]
            for arg in args[1:]:
                # axis = 1 to drop column not index
                result = result.drop(arg, 1)
        return result

    global switch
    switch = {
        "equal": equal,
        "gt": gt,
        "lt": lt,
        "goe": goe,
        "loe": loe,
        "col": col,
        "exc": exc,
        "or": alt,
        "and": conj
    }

    def switcher(self, fun, args):
        func = switch.get(fun)
        return func(self, args)

    def exec_recurrent(self, query):
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
                part = self.exec_recurrent(temp_query)
                if type(arg) is pd.DataFrame:
                    pos += 1
                    continue
                else:
                    res_args.pop(pos)
                    res_args.insert(pos, part)
                    pos += 1
            part_result[0][fun] = res_args
            result = self.exec_recurrent(part_result)
        else:
            result = self.switcher(fun, args)

        return result
