import regex
import json
from pathlib import Path


class QueryParser:

    def __init__(self, search):
        self.query = self.__separate(search)
        self.source = self.query[0] # separate func returns table , where 1st element is source
        self.parsed = self.__parse(self.query[1])
        self.__validate(self.parsed)
        # if there is no error parsed is valid list of functions
        self.functions = self.parsed

    def get_source(self):
        return self.source

    def get_func(self):
        return self.functions

    def __separate(self, query):
        # checks query pattern and separates source name, from functions part of query
        pattern = r'source\((?P<args>(?:[^\(\)])*)\)'  # source pattern
        match = regex.search(pattern, query)
        # search source pattern in query
        if match is None:
            raise Exception('Incorrect source format!')
        s = match.group('args').replace('"', '')
        # returns a part with match(source) and gets rid of " signs
        source = s.split(',')
        # split sources (if there's more than one source) to a list
        func = query[match.end()+1:]
        # func => query starting from sign after match(source) ends
        return source, func

    # syntactic analysis of query
    def __parse(self, search):
        UNQUOTED_NAME = r'(?:\w[[:alnum:]_]*)'
        QUOTED_NAME = r'(?:\"(?:\\.|[^\"])*\")'

        NAME = r'(?P<name>%s|%s)' % (UNQUOTED_NAME, QUOTED_NAME)
        pattern = r'(?P<fullmatch>%s?(?:\((?P<args>(?:[^\(\)]|(?R))*)\))?)' % NAME
        p = regex.compile(pattern)
        match = regex.search(pattern, search)
        if match.group('name') is None:
            raise Exception('Incorrect query! Syntactic analysis failed!')
        result = []
        for m in p.finditer(search):
            tmp = m.groupdict()
            # to avoid adding empty matches to result array
            if tmp['fullmatch'] == '':
                continue
            elif tmp['args'] and "," in tmp['args']:
                tmp['args'] = self.__parse(tmp['args'])
                temp = []
                for arg in tmp['args']:
                    if '"' in arg:
                        last_char = len(arg) - 1
                        arg = arg[1:last_char]
                        temp.append(arg.replace('\"', '"'))
                    else:
                        temp.append(arg)
                result.append({tmp['name']: temp})
            elif "(" in tmp['fullmatch']:
                temp = []
                if type(tmp['args'] is str):
                    arg = tmp['args']
                    if '"' in tmp["args"]:
                        last_char = len(arg) - 1
                        arg = arg[1:last_char]
                        temp.append(arg.replace('\"', '"'))
                    else:
                        temp.append(tmp['args'])
                else:
                    for arg in tmp['args']:
                        if '"' in arg:
                            last_char = len(arg)-1
                            arg = arg[1:last_char]
                            temp.append(arg.replace('\"', '"'))
                        else:
                            temp.append(arg)
                result.append({tmp['name']: temp})
            else:
                result.append(tmp['name'])
        return result

    # go thru search tree to check if func have a valid number of args
    def __validate(self, search_tree):
        path = Path(__file__).resolve().parents[1]  # partial path to json schema
        result = True
        for edge in search_tree:
            if type(edge) == str:  # continue if argument is string not a func, maximum depth level
                continue
            fun = list(edge.keys())[0]
            # fun => dict key from tree edge is a function name
            result = self.__validate(edge[fun])
            if not result:
                raise Exception('Incorrect query! Some function has invalid number of arguments!')
            schema = json.load(open("%s\\etc\\functions\\%s.json" % (path, fun)))
            # loads function schema from json file
            if (int(schema.get('maxNumberOfArguments')) == -1) & (int(schema.get('minNumberOfArguments')) <= len(
                    edge[fun])):  # if func can have infinite number of args, just check if it has more than min
                result = True
            # check min and max number of args
            elif int(schema.get('minNumberOfArguments')) <= len(edge[fun]) <= int(schema.get('maxNumberOfArguments')):
                result = True
            else:
                raise Exception('Incorrect query! Some function has invalid number of arguments!')
        return result
