"""
Analyzes SQL scripts to infer performed actions
"""
# TODO: move this to static_analysis
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


def name_from_create_statement(statement):
    # NOTE: is there a better way to look for errors?
    errors = [
        t.ttype for t in statement.tokens if 'Token.Error' in str(t.ttype)
    ]

    if any(errors):
        warnings.warn('Failed to parse statement: {}'.format(str(statement)))
    else:
        # after removing whitespace this should be
        # [CREATE, 'TABLE|VIEW', 'IF EXISTS'?, 'IDENTIFIER']
        elements = [t for t in statement.tokens if not t.is_whitespace]

        # get the first identifier and extract tokens
        tokens = [
            e for e in elements if isinstance(e, sqlparse.sql.Identifier)
        ][0].tokens

        if len(tokens) == 1:
            return ParsedSQLRelation(schema=None,
                                     name=tokens[0].value,
                                     kind=str(elements[1]))

        else:
            return ParsedSQLRelation(schema=tokens[0].value,
                                     name=tokens[2].value,
                                     kind=str(elements[1]))


def created_relations(sql, split_source=None):
    """
    Determine the name of the relations that will be created if we run a given
    sql script

    Parameters
    ----------
    sql : str
        SQL code

    split_source : str
        The character used to delimit multiple commands. If not None, each
        command is analyzed separately and assumed to be executed in order
    """
    # the split_source argument makes the database client able to execute
    # multiple statements at once even if the driver does not allow it, by
    # splitting by a character (usually ';') and executing one task at a time.
    # there is one special case where one might want to use a different
    # character, this will cause sqlparse.parse to fail so we have to split
    # and re-join using ';' which is an acceptable delimiter in SQL
    if split_source:
        sql = ';'.join(sql.split(split_source))

    sql = sqlparse.format(sql,
                          keyword_case='lower',
                          identifier_case='lower',
                          strip_comments=True)
    statements = sqlparse.parse(sql)

    # get DROP and CREATE statements along with their indexes, which give
    # the position
    drop_idx = {
        name_from_create_statement(s): idx
        for idx, s in enumerate(statements) if s.get_type() == 'DROP'
    }
    create_idx = {
        name_from_create_statement(s): idx
        for idx, s in enumerate(statements) if s.get_type() == 'CREATE'
    }

    relations = []

    # add a relation to the final list if...
    for relation, idx in create_idx.items():
        # there isn't a DROP statement with the same name
        # OR
        # there is a DROP statement but comes before the CREATE
        # e.g. DROP TABLE x; CREATE TABLE x AS ...
        if relation not in drop_idx or drop_idx[relation] < idx:
            relations.append(relation)

    return relations
