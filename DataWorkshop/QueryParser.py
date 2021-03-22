import regex
import json
from pathlib import Path


class QueryParser:

    def __init__(self, search):
        query = self.__separate(search)
        self.source = query[0]
        parsed = self.__parse(query[1])
        self.__validate(parsed)
        # if there is no error parsed is valid list of functions
        self.functions = parsed

    def get_source(self):
        return self.source

    def get_func(self):
        return self.functions

    def __separate(self, query):
        pattern = r'source\((?P<args>(?:[^\(\)])*)\)'
        match = regex.search(pattern, query)
        if match is None:
            raise Exception('Incorrect source format!')
        s = match.group('args').replace('"', '')
        source = s.split(',')
        func = query[match.end()+1:]
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

    # semantic analysis of search tree
    def __validate(self, search_tree):
        # partial path to json schema
        path = Path(__file__).resolve().parents[1]
        result = True
        for edge in search_tree:
            if type(edge) == str:
                continue
            fun = list(edge.keys())[0]
            result = self.__validate(edge[fun])
            if not result:
                raise Exception('Incorrect query! Semantic analysis failed!')
            schema = json.load(open("%s\\etc\\functions\\%s.json" % (path, fun)))
            if (int(schema.get('maxNumberOfArguments')) == -1) & (int(schema.get('minNumberOfArguments')) <= len(
                    edge[fun])):
                result = True
            elif int(schema.get('minNumberOfArguments')) <= len(edge[fun]) <= int(schema.get('maxNumberOfArguments')):
                result = True
            else:
                raise Exception('Incorrect query! Semantic analysis failed!')
        return result
