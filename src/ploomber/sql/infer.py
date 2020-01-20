"""
Analyzes SQL scripts to infer performed actions
"""
import warnings
import sqlparse


class ParsedSQLRelation:
    """
    This is similar to the SQLRelationPlaceholder, it enables operations
    such as comparisons. Not using SQLRelationPlaceholder directly to
    avoid complex imports
    """

    def __init__(self, schema, name, kind):
        if schema is not None:
            schema.replace('"', '')

        self.schema = schema
        self.name = name.replace('"', '')
        self.kind = kind

    def __eq__(self, other):
        return (self.schema == other.schema
                and self.name == other.name
                and self.kind == other.kind)

    def __hash__(self):
        return hash((self.schema, self.name, self.kind))


def parse_statement(statement):
    # NOTE: is there a better way to look for errors?
    errors = [t.ttype for t in statement.tokens
              if 'Token.Error' in str(t.ttype)]

    if any(errors):
        warnings.warn('Failed to parse statement: {}'.format(str(statement)))
    else:
        # after removing whitespace this should be [CREATE, 'TABLE|VIEW', 'ID']
        elements = [t for t in statement.tokens if not t.is_whitespace]

        identifier = str(elements[2])
        tokens = identifier.split('.')

        if len(tokens) == 1:
            schema = None
            name = tokens[0]
        else:
            schema, name = tokens

        return ParsedSQLRelation(schema=schema, name=name,
                                 kind=str(elements[1]))


def created_relations(sql):
    sql = sqlparse.format(sql, keyword_case='lower',
                          identifier_case='lower',
                          strip_comments=True)
    statements = sqlparse.parse(sql)

    drop = [parse_statement(s) for s in statements
            if s.get_type() == 'DROP']
    create = [parse_statement(s) for s in statements
              if s.get_type() == 'CREATE']

    return list(set(create) - set(drop))
