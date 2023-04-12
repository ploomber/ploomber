from copy import copy

from jinja2 import Template
import sqlparse


def strip(tokens):
    """
    Remove whitespace, comments and punctuation (;)
    """
    return [
        t
        for t in tokens
        if not t.is_whitespace
        and not isinstance(t, sqlparse.sql.Comment)
        and t.value != ";"
    ]


def strip_punctuation(tokens):
    return [t for t in tokens if t.value != ";"]


def strip_comments(tokens):
    return [t for t in tokens if not isinstance(t, sqlparse.sql.Comment)]


def code_from_token_list(tokens):
    return "".join([t.value for t in tokens])


def name_code_pair(f):
    # split to: ({name}, AS, {code})
    t_name, _, t_code = strip(f)
    name = t_name.value
    # ignore parenthesis
    code = code_from_token_list(t_code.tokens[1:-1])
    # NOTE: probably better to strip during SQLParser init?
    return name, sqlparse.format(code, strip_comments=True).strip()


def find_select(tokens):
    for idx, t in enumerate(tokens):
        # TODO: use .get_type instead of normalized
        if t.normalized == "SELECT":
            return idx


def find_with_type(tokens, type_):
    for idx, t in enumerate(tokens):
        if t.get_type() == type_:
            return idx, t


def find_with_class(tokens, class_):
    for idx, t in enumerate(tokens):
        if isinstance(t, class_):
            return idx, t


def get_identifiers_and_select_from_create(create):
    res = find_with_class(create, class_=sqlparse.sql.IdentifierList)

    # when CREATE TABLE name has no parenthesis...
    if res:
        idx = find_select(create)
        select = code_from_token_list(strip_punctuation(create.tokens[idx:]))
        return strip(res[1]), select

    # when there are parenthesis
    _, identifier = find_with_class(create, class_=sqlparse.sql.Identifier)
    # last one is the parenthesis object that encloses the identifier list
    parenthesis = identifier.tokens[-1]
    # ignore parenthesis tokens at the beginning and end
    _, id_list = find_with_class(parenthesis, class_=sqlparse.sql.IdentifierList)

    idx = find_select(parenthesis)
    # ignore last token, it's the closing parenthesis
    select = code_from_token_list(strip_punctuation(parenthesis.tokens[idx:-1]))

    return strip(id_list), select


def find_identifiers_list(sql):
    # find the first of those (identifier happens when there is a single def)
    id_list = find_with_class(
        sql, class_=(sqlparse.sql.Identifier, sqlparse.sql.IdentifierList)
    )

    # if one identifier, wrap it as a list of length 1
    if isinstance(id_list[1], sqlparse.sql.Identifier):
        return [id_list[1]]
    else:
        return id_list[1]


def get_identifiers_and_select_from_with_select(with_select):
    defs = find_identifiers_list(with_select)

    idx = find_select(with_select)
    select = code_from_token_list(strip_punctuation(with_select.tokens[idx:]))

    return strip(defs), select


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

    CREATE [TABLE|VIEW] is optional.

    If there is more than one statement, the first CREATE [TABLE|VIEW] is used.
    Get individual SELECT statements using parser['step_a']

    Attributes
    ----------
    mapping : dict
        Contains the extracted subqueries
    """

    def __init__(self, sql):
        if sql is not None:
            # get the first create table statement
            out = find_with_type(sqlparse.parse(sql), type_="CREATE")

            if out is not None:
                # there is a create statement
                _, create = out

                # find the list of identifiers and last select statement
                ids, select = get_identifiers_and_select_from_create(create)
            else:
                # no create statement, look for the WITH ... SELECT statement
                _, with_select = find_with_type(sqlparse.parse(sql), type_="SELECT")
                ids, select = get_identifiers_and_select_from_with_select(with_select)

            # this should be
            # {identifier} AS ( SELECT ... )
            # then punctiation ","
            # and repeat...
            functions = [id_ for idx, id_ in enumerate(ids) if not idx % 2]

            functions_t = [name_code_pair(f) for f in functions]

            m = {t[0]: t[1] for t in functions_t}

            m["_select"] = select

            self.mapping = m
        else:
            self.mapping = dict()

    def __getitem__(self, key):
        """Same as .until(key)"""
        return self.until(key)

    def _ipython_key_completions_(self):
        return list(self)

    def __setitem__(self, key, value):
        self.mapping[key] = value

    def __iter__(self):
        for e in self.mapping:
            yield e

    def __len__(self):
        return len(self.mapping)

    def __repr__(self):
        return f"{type(self).__name__} with keys: {list(self.mapping)!r}"

    def until(self, key, select=None, limit=20, parse=True):
        """
        Generate with statements until the one with the given identifier.
        Adding a final select statement which can be customized with the
        "select" parameter, if "select" is None, the last select statement
        is used with a LIMIT statement. If you pass a custom select statement,
        you can include the {{key}} placeholder which is replaced by the key
        argument.
        """
        pairs = []

        for a_key in self.mapping:
            pairs.append((a_key, self.mapping[a_key]))

            if a_key == key:
                break

        if key == "_select":
            _, select = pairs.pop(-1)
        else:
            if select is None:
                select = f"SELECT * FROM {pairs[-1][0]}"

                if limit:
                    select += f" LIMIT {limit}"
            else:
                select = Template(select).render(key=key)

        sql = Template(
            """
WITH {%- for id,code in pairs -%}{{',' if not loop.first else '' }} {{id}} AS (
    {{code}}
){% endfor %}
{{select}}"""
        ).render(pairs=pairs, select=select)

        return sql if not parse else type(self)(sql)

    @classmethod
    def _with_mapping(cls, mapping):
        obj = cls(sql=None)
        obj.mapping = mapping
        return obj

    def insert_first(self, key, select, inplace=False):
        mapping = {key: select, **self.mapping}

        if inplace:
            self.mapping = mapping
            return self
        else:
            return type(self)._with_mapping(mapping)

    def insert_last(self, select, inplace=False):
        m = copy(self.mapping)
        m["last"] = m.pop("_select")
        mapping = {**m, "_select": select}

        if inplace:
            self.mapping = mapping
            return self
        else:
            return type(self)._with_mapping(mapping)

    def replace_last(self, select, inplace=False):
        mapping = copy(self.mapping)
        mapping["_select"] = select

        if inplace:
            self.mapping = mapping
            return self
        else:
            return type(self)._with_mapping(mapping)

    def __str__(self):
        """Short for .until(key='_select')"""
        return self.to_str(select=None, limit=None)

    def to_str(self, select=None, limit=20):
        """Short for .until(key=None, *args, **kwargs)"""
        return self.until(key="_select", select=select, limit=limit, parse=False)
