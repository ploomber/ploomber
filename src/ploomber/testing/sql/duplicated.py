from typing import Union, List

from jinja2 import Template
from tabulate import tabulate

from ploomber.util.util import _make_iterable


def _duplicates_query(col, product):
    """Generate SQL code that counts number of duplicates
    """
    cols = ','.join(_make_iterable(col))

    return Template("""
    SELECT {{cols}}, COUNT(*) - 1 AS n_duplicates
    FROM {{product}}
    GROUP BY {{cols}}
    HAVING COUNT(*) > 1
    """).render(cols=cols, product=product)


def duplicates_in_column(client, col: Union[str, List[str]], product) -> bool:
    """Check if a column (or group of columns) has duplicated values

    Parameters
    ----------
    client
        Database client
    cols
        Column(s) to check
    product
        The relation to check

    Returns
    -------
    bool
        True if there are duplicates in the column(s). If passed more than
        one column, they are considered as a whole, not individually
    """
    sql = Template("""
    SELECT EXISTS(
        {{query}}
    )
    """).render(query=_duplicates_query(col, product))

    cur = client.connection.cursor()
    cur.execute(sql)
    output = bool(cur.fetchone()[0])
    cur.close()

    return output


def assert_no_duplicates_in_column(client,
                                   col: Union[str, List[str]],
                                   product,
                                   stats=False):
    """
    Assert if there are duplicates in a column (or group of columns). If there
    are duplicates, it raises an AssertionError with an error message showing
    some of the duplicated values

    Parameters
    ----------
    stats : bool, default=False
        Whether to show duplicates stats in the error message or not
    """
    duplicates_query = _duplicates_query(col, product)

    sql = Template("""
    {{query}}
    LIMIT 10
    """).render(query=duplicates_query)

    cur = client.connection.cursor()
    cur.execute(sql)
    output = cur.fetchall()

    if len(output):
        names = [t[0] for t in cur.description]
        table = tabulate(output, headers=names)
        cols = ','.join(_make_iterable(col))

        sql_sample_rows = Template("""
        WITH duplicated AS (
            {{sql}}
        )
        SELECT t.*
        FROM {{product}} AS t
        JOIN duplicated
        USING ({{cols}})
        ORDER BY {{cols}}
        """).render(sql=sql, product=product, cols=cols)

        cur = client.connection.cursor()
        cur.execute(sql_sample_rows)
        output = cur.fetchall()
        cur.close()

        names = [t[0] for t in cur.description]
        table_sample = tabulate(output, headers=names)

        msg = f'Duplicates found.\n\n{table}\n\n{table_sample}'

        if stats:
            n_rows, n_unique, n_duplicates = duplicates_stats(
                client, col, product)

            msg += (f'\n\nNumber of rows: {n_rows:,}\n'
                    f'Number of unique values: {n_unique:,}\n'
                    f'Number of duplicates: {n_duplicates:,}')

        raise AssertionError(msg)


def duplicates_stats(client, col: Union[str, List[str]], product):
    """Get stats on rows with duplicated values

    Returns
    -------
    n_rows
        Number of rows in product
    n_unique
        Number of unique values (for selected columns) in product
    n_duplicates
        Number of rows with duplicated values (this is equal as the number
        of rows we'd have to drop to remove duplicates)
    """
    cols = ','.join(_make_iterable(col))

    # num of rows in product
    n_rows = _query(
        client,
        Template('SELECT COUNT(*) FROM {{product}}').render(product=product))

    # num of unique values (using all columns)
    n_unique = _query(
        client,
        Template('SELECT COUNT(DISTINCT({{cols}})) FROM {{product}}').render(
            product=product, cols=cols))

    sql_n_duplicates = Template("""
        WITH duplicated AS (
            {{sql}}
        )
        SELECT SUM(n_duplicates) FROM duplicated
        """).render(sql=_duplicates_query(col, product),
                    product=product,
                    cols=cols)

    # num of duplicated rows (number of rows we have to drop to remove all
    # duplicates)
    n_duplicates = _query(client, sql_n_duplicates)

    return n_rows, n_unique, n_duplicates


def _query(client, sql):
    cur = client.connection.cursor()
    cur.execute(sql)
    res = cur.fetchone()[0]
    cur.close()
    return res
