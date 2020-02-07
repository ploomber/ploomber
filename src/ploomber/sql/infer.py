"""
Analyzes SQL scripts to infer performed actions
"""
import warnings
import sqlparse


def _quoted_with(identifier, char):
    return identifier.startswith(char) and identifier.endswith(char)


def _normalize(identifier):
    """
    Normalize a SQL identifier. Given that different SQL implementations have
    different rules, we will implement logic based on PostgreSQL. Double
    quotes make an identifier case sensitive, unquoted are folded to lower
    case. MySQL on the other hand, depends on the file system, furthermore
    the quoting identifier character can also be a backtick.

    Notes
    -----
    PostgreSQL - Section 4.1.1 mentions quoted identifiers:
    https://www.postgresql.org/docs/9.1/sql-syntax-lexical.html

    MySQL - Section 9.2:
    https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
    https://dev.mysql.com/doc/refman/8.0/en/identifier-case-sensitivity.html
    """
    # does this cover all use cases?
    if identifier is None:
        return None
    elif _quoted_with(identifier, '"'):
        return identifier.replace('"', '')
    else:
        return identifier.lower()


class ParsedSQLRelation:
    """
    This is similar to the SQLRelationPlaceholder, it enables operations
    such as comparisons. Not using SQLRelationPlaceholder directly to
    avoid complex imports
    """

    def __init__(self, schema, name, kind):
        self.schema = schema
        self.name = name
        self.kind = kind

    def __eq__(self, other):
        return (_normalize(self.schema) == _normalize(other.schema)
                and _normalize(self.name) == _normalize(other.name)
                and _normalize(self.kind) == _normalize(other.kind))

    def __str__(self):
        if self.schema is None:
            return self.name
        else:
            return '{}.{}'.format(self.schema, self.name)

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, str(self))

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

        if not tokens or len(tokens) > 2:
            # warnings.warn('Failed to parse statement: {}'
                            # .format(str(statement)))
            return None

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
