from jinja2 import Template
import sqlparse


def strip(tokens):
    return [
        t for t in tokens
        if not t.is_whitespace and not isinstance(t, sqlparse.sql.Comment)
    ]


def strip_comments(tokens):
    return [t for t in tokens if not isinstance(t, sqlparse.sql.Comment)]


def code_from_token_list(tokens):
    return ''.join([t.value for t in tokens])


def to_tuple(f):
    t_name, _, t_code = strip(f)
    name = t_name.value
    # ignore parenthesis
    code = code_from_token_list(t_code.tokens[1:-1])
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


class SQLParser:
    def __init__(self, sql):
        _, t = find_with_type(sqlparse.parse(sql), type_='CREATE')

        identifiers = strip(t)[-1]
        parenthesis = identifiers.tokens[-1]

        # with statement
        with_ = parenthesis.tokens[1:-1]

        tokens = strip(with_)
        # this must be the with statement
        # tokens[0]

        # this must be the list of expressions
        ids = strip(tokens[1].tokens)

        # this should be function, punctuation, function, puntuaction...
        functions = [id_ for idx, id_ in enumerate(ids) if not idx % 2]

        functions_t = [to_tuple(f) for f in functions]

        m = {t[0]: t[1] for t in functions_t}

        # the rest is part of the final select statement, we don't use it bc it has
        # whitespace removed
        # tokens[2:]

        idx = find_select(with_)

        m['_select'] = code_from_token_list(with_[idx:])

        self.mapping = m

    def __getitem__(self, key):
        return self.mapping[key]

    def __iter__(self):
        for e in self.mapping:
            yield e

    def __len__(self):
        return len(self.mapping)

    def __repr__(self):
        return f'{type(self).__name__} with keys: {list(self.mapping)!r}'

    def until(self, key):
        pairs = []

        for a_key in self.mapping:
            pairs.append((a_key, self.mapping[a_key]))

            if a_key == key:
                break

        sql = Template("""
WITH {%- for id, code in pairs -%}{{',' if not loop.first else '' }} {{id}} as (
    {{code}}
){% endfor %}
SELECT * FROM {{pairs[-1][0]}}""").render(pairs=pairs)

        return sql
