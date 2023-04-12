import warnings

import sqlparse

from ploomber import products
from ploomber.static_analysis.abc import Extractor
from ploomber.static_analysis.jinja import JinjaExtractor


class SQLExtractor(Extractor):
    """Extract upstream and product from SQL templates

    Parameters
    ----------
    code : str or Placeholder
        SQL code
    """

    def __init__(self, code):
        self._jinja_extractor = JinjaExtractor(code)
        self._product = None
        self._extracted_product = False

    def extract_upstream(self):
        """Extract upstream keys used in a templated SQL script"""
        return self._jinja_extractor.find_variable_access(variable="upstream")

    def extract_product(self, raise_if_none=True):
        """
        Extract an object from a SQL template that defines as product variable:

        {% set product = SOME_CLASS(...) %}

        Where SOME_CLASS is a class defined in ploomber.products. If no product
        variable is defined, returns None
        """
        product = self._jinja_extractor.find_variable_assignment(variable="product")

        if product is None:
            if raise_if_none:
                code = self._jinja_extractor.get_code_as_str()
                raise ValueError(f"Couldn't extract 'product' from code: {code!r}")
        else:
            # validate product
            try:
                # get the class name used
                class_ = getattr(products, product.node.name)
                # get the arg passed to the class
                arg = product.args[0].as_const()
                # try to initialize object
                return class_(arg)
            except Exception as e:
                exc = ValueError(
                    "Found a variable named 'product' in "
                    "code: {} but it does not appear to "
                    "be a valid SQL product, verify it ".format(
                        self._jinja_extractor.code
                    )
                )
                raise exc from e


def _quoted_with(identifier, char):
    return identifier.startswith(char) and identifier.endswith(char)


def _normalize(identifier):
    """
    Normalize a SQL identifier. Given that different SQL implementations have
    different rules, we will implement logic based on PostgreSQL. Double
    quotes make an identifier case sensitive, unquoted are forced to lower
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
        return identifier.replace('"', "")
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
        return (
            _normalize(self.schema) == _normalize(other.schema)
            and _normalize(self.name) == _normalize(other.name)
            and _normalize(self.kind) == _normalize(other.kind)
        )

    def __str__(self):
        if self.schema is None:
            return self.name
        else:
            return "{}.{}".format(self.schema, self.name)

    def __repr__(self):
        raw_repr = (
            (self.name, self.kind)
            if self.schema is None
            else (self.schema, self.name, self.kind)
        )
        return f"{type(self).__name__}({raw_repr!r})"

    def __hash__(self):
        return hash((self.schema, self.name, self.kind))


def name_from_create_statement(statement):
    # NOTE: is there a better way to look for errors?
    errors = [t.ttype for t in statement.tokens if "Token.Error" in str(t.ttype)]

    if any(errors):
        warnings.warn("Failed to parse statement: {}".format(str(statement)))
    else:
        # after removing whitespace this should be
        # [CREATE, 'TABLE|VIEW', 'IF EXISTS'?, 'IDENTIFIER']
        elements = [t for t in statement.tokens if not t.is_whitespace]

        # get the first identifier and extract tokens
        tokens = [e for e in elements if isinstance(e, sqlparse.sql.Identifier)][
            0
        ].tokens

        if len(tokens) == 1:
            return ParsedSQLRelation(
                schema=None, name=tokens[0].value, kind=str(elements[1])
            )

        else:
            return ParsedSQLRelation(
                schema=tokens[0].value, name=tokens[2].value, kind=str(elements[1])
            )


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
        sql = ";".join(sql.split(split_source))

    sql = sqlparse.format(
        sql, keyword_case="lower", identifier_case="lower", strip_comments=True
    )
    statements = sqlparse.parse(sql)

    # get DROP and CREATE statements along with their indexes, which give
    # the position
    drop_idx = {
        name_from_create_statement(s): idx
        for idx, s in enumerate(statements)
        if s.get_type() == "DROP"
    }
    create_idx = {
        name_from_create_statement(s): idx
        for idx, s in enumerate(statements)
        if s.get_type() == "CREATE"
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
