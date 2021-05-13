import json
import copy
from .Connector import Connector
import sqlite3


class SQLConnector(Connector):

    def __init__(self, access):
        con = sqlite3.connect(access.get('db_name'))
        self.cur = con.cursor()
        self.tab_name = access.get('table_name')

    def execute(self, query):
        # necessary to work on copy not a real query
        query_copy = copy.deepcopy(query)
        fun = list(query[0].keys())[0]
        args = query_copy[0][fun]
        formatted = self.format_query(query_copy)
        if fun == 'col':
            final_query = self.col(args)
        elif fun == 'exc':
            final_query = self.exc(args)
        else:
            final_query = 'SELECT * FROM ' + self.tab_name + ' WHERE ' + formatted

        rows = self.cur.execute(final_query).fetchall()
        keys = [tup[0] for tup in self.cur.description]

        json_result = [dict((keys[i], value) for i, value in enumerate(row)) for row in rows]

        return json_result

    def equal(self, args):
        if type(args[1]) is str:
            res = '"' + args[1] + '"' + '=' + '"' + args[1] + '"'
        else:
            res = '"' + args[1] + '"' + '=' + args[1]

        return res

    def ne(self, args):
        if type(args[1]) is str:
            res = '"' + args[0] + '"' + '!=' + '"' + args[1] + '"'
        else:
            res = '"' + args[0] + '"' + '!=' + args[1]

        return res

    def gt(self, args):
        res = '"' + args[0] + '"' + '>' + args[1]
        return res

    def lt(self, args):
        res = '"' + args[0] + '"' + '<' + args[1]
        return res

    def goe(self, args):
        res = '"' + args[0] + '"' + '>=' + args[1]
        return res

    def loe(self, args):
        res = '"' + args[0] + '"' + '<=' + args[1]
        return res

    def alt(self, args):
        part = ''
        for arg in args:
            part = part + arg + ' OR '
        last_pos = len(part) - 4
        part = part[0: last_pos]
        return part

    def conj(self, args):
        part = ''
        for arg in args:
            part = part + arg + ' AND '
        last_pos = len(part) - 5
        part = part[0: last_pos]
        return part

    def col(self, args):
        sql_query = 'SELECT '
        check = copy.deepcopy(args[0])
        if (check.find('=') != -1) or (check.find('>') != -1) or (check.find('<') != -1):
            for arg in args[1:]:
                sql_query += '"' + arg + '", '
            sql_query = sql_query[:len(sql_query)-2]
            sql_query += ' FROM ' + self.tab_name + ' WHERE ' + args[0]
        else:
            for arg in args:
                sql_query += '"' + arg + '", '
            sql_query = sql_query[:len(sql_query) - 2]
            sql_query += ' FROM ' + self.tab_name

        return sql_query

    # except function
    def exc(self, args):
        self.cur.execute('SELECT * FROM ' + self.tab_name)
        col_names = [tuple[0] for tuple in self.cur.description]
        check = copy.deepcopy(args[0])
        # to get to know if need to choose from whole datatbase or partial result
        comp = False

        # check if any of signs: '<>=' is present in arg
        if (check.find('=') != -1) or (check.find('>') != -1) or (check.find('<') != -1):
            comp = True;
            for arg in args[1:]:
                col_names.remove(arg)
        else:
            for arg in args:
                col_names.remove(arg)
        sql_query = 'SELECT '
        for name in col_names:
            sql_query += '"' + name + '", '
        sql_query = sql_query[:len(sql_query) - 2]
        sql_query += ' FROM ' + self.tab_name
        if (comp):
            sql_query += ' WHERE ' + args[0]

        return sql_query

    switch = {
        "equal": equal,
        "ne": ne,
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
        func = self.switch.get(fun)
        return func(self, args)

    def format_query(self, query):
        result = None
        res_query = query
        fun = list(query[0].keys())[0]
        args = query[0][fun]
        if any(type(arg) is dict for arg in args):
            pos = 0
            for arg in args:
                if type(arg) is str:
                    pos += 1
                    continue
                # query must be a list of dicts (arg is dict)
                temp_query = [arg]
                part = self.format_query(temp_query)
                pos = args.index(arg)
                args.pop(pos)
                args.insert(pos, part)
            res_query[0][fun] = args
            result = self.format_query(res_query)
        elif all(type(arg) is str for arg in args):
            result = self.switcher(fun, args)
        return result
