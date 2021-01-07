from copy import copy

from jinja2 import Template
import sqlparse


def strip(tokens):
    """
    Remove whitespace, comments and punctuation (;)
    """
    return [
        t for t in tokens if not t.is_whitespace
        and not isinstance(t, sqlparse.sql.Comment) and t.value != ';'
    ]


def strip_punctuation(tokens):
    return [t for t in tokens if t.value != ';']


def strip_comments(tokens):
    return [t for t in tokens if not isinstance(t, sqlparse.sql.Comment)]


def code_from_token_list(tokens):
    return ''.join([t.value for t in tokens])


def to_tuple(f):
    t_name, _, t_code = strip(f)
    name = t_name.value
    # ignore parenthesis
    code = code_from_token_list(t_code.tokens[1:-1])
    # NOTE: probably better to strip during SQLParser init?
    return name, sqlparse.format(code, strip_comments=True).strip()


def find_select(tokens):
    for idx, t in enumerate(tokens):
        # TODO: use .get_type instead of normalized
        if t.normalized == 'SELECT':
            return idx


def find_with_type(tokens, type_):
    for idx, t in enumerate(tokens):
        if t.get_type() == type_:
            return idx, t


def find_with_class(tokens, class_):
    for idx, t in enumerate(tokens):
        if isinstance(t, class_):
            return idx, t


def find_identifiers_list(create):
    # when CREATE TABLE name has no parenthesis...
    res = find_with_class(create, class_=sqlparse.sql.IdentifierList)

    if res:
        idx = find_select(create)
        select = code_from_token_list(strip_punctuation(create.tokens[idx:]))
        return strip(res[1]), select

    # when there are parenthesis
    _, identifier = find_with_class(create, class_=sqlparse.sql.Identifier)
    # last one is the parenthesis object that encloses the identifier list
    parenthesis = identifier.tokens[-1]
    # ignore parenthesis tokens at the beginning and end
    _, id_list = find_with_class(parenthesis,
                                 class_=sqlparse.sql.IdentifierList)

    idx = find_select(parenthesis)
    # ignore last token, it's the closing parenthesis
    select = code_from_token_list(strip_punctuation(
        parenthesis.tokens[idx:-1]))

    return strip(id_list), select


class SQLParser:
    """Parse a SQL script with format:

    CREATE [TABLE|VIEW] something  AS (
        WITH step_a AS (
            SELECT * FROM a
        ), step_b AS (
            SELECT * FROM B
        ), ...

        SELECT * FROM ...
    )

    If there is more than one statement, the first CREATE [TABLE|VIEW] is used.
    Get individual SELECT statements using parser['step_a']

    """
    def __init__(self, sql):
        if sql is not None:
            # from IPython import embed
            # embed()

            # get the first create table statement
            _, create = find_with_type(sqlparse.parse(sql), type_='CREATE')

            # find the list of identifiers and last select statement
            ids, select = find_identifiers_list(create)

            # this should be function, punctuation, function, puntuaction...
            functions = [id_ for idx, id_ in enumerate(ids) if not idx % 2]

            functions_t = [to_tuple(f) for f in functions]

            m = {t[0]: t[1] for t in functions_t}

            m['_select'] = select

            self.mapping = m
        else:
            self.mapping = dict()

    def __getitem__(self, key):
        return self.mapping[key]

    def __setitem__(self, key, value):
        self.mapping[key] = value

    def __iter__(self):
        for e in self.mapping:
            yield e

    def __len__(self):
        return len(self.mapping)

    def __repr__(self):
        return f'{type(self).__name__} with keys: {list(self.mapping)!r}'

    def until(self, key, select=None, limit=20):
        """
        Generate with statements until the one with the given identifier.
        Adding a final "SELECT * {{key}}" which can be customized with the
        "select" parameter, if "select" is None, the last select statement
        is used with a LIMIT statement
        """
        pairs = []

        # _select should never be included
        mapping = copy(self.mapping)
        del mapping['_select']

        for a_key in mapping:
            pairs.append((a_key, mapping[a_key]))

            if a_key == key:
                break

        if select is None:
            select = f'SELECT * FROM {pairs[-1][0]}'

            if limit:
                select += f' LIMIT {limit}'

        sql = Template("""
WITH {%- for id, code in pairs -%}{{',' if not loop.first else '' }} {{id}} as (
    {{code}}
){% endfor %}
{{select}}""").render(pairs=pairs, select=select)

        return sql

    @classmethod
    def _with_mapping(cls, mapping):
        obj = cls(sql=None)
        obj.mapping = mapping
        return obj

    def insert(self, key, code, inplace=True):
        """Insert a new (identifier, code) pair at the beginning
        """
        mapping = {**{key: code}, **self.mapping}

        if inplace:
            self.mapping = mapping
            return self
        else:
            return type(self)._with_mapping(mapping)
