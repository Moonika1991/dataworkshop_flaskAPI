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
            col = args[0]
            if fun == 'equal':
                if type(args[1]) is str:
                    result = '"' + col + '"' + '=' + '"' + args[1] + '"'
                else:
                    result = '"' + col + '"' + '=' + args[1]
            elif fun == 'gt':
                result = '"' + col + '"' + '>' + args[1]
            elif fun == 'lt':
                result = '"' + col + '"' '<' + args[1]
            elif fun == 'goe':
                result = '"' + col + '"' + '>=' + args[1]
            elif fun == 'loe':
                result = '"' + col + '"' '<=' + args[1]
            elif fun == 'or':
                result = self.alt(args)
            elif fun == 'and':
                result = self.conj(args)
            else:
                result = res_query
        return result

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
