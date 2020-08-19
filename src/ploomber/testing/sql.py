"""
Function for testing testing SQL relations
"""
from jinja2 import Template
from ploomber.util.util import _make_iterable
from typing import Union, List


def nulls_in_columns(client, cols: Union[str, List[str]], product):
    """Check if any column has NULL values, returns bool

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
        True if there is at least one NULL in any of the columns
    """
    sql = Template("""
    SELECT EXISTS(
        SELECT * FROM {{product}}
        WHERE {{cols | join(' is null or ') }} is null
    )
    """).render(cols=cols, product=product)

    cur = client.connection.cursor()
    cur.execute(sql)
    output = bool(cur.fetchone()[0])
    cur.close()

    return output


def distinct_values_in_column(client, col: str, product):
    """Get distinct values in a column

    Parameters
    ----------
    client
        Database client
    col
        Column to check
    product
        The relation to check

    Returns
    -------
    set
       Distinct values in column
    """

    sql = Template("""
    SELECT DISTINCT {{col}} FROM {{product}}
    """).render(col=col, product=product)

    cur = client.connection.cursor()
    cur.execute(sql)
    output = cur.fetchall()
    cur.close()

    return set(o[0] for o in output)


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
    cols = ','.join(_make_iterable(col))

    sql = Template("""
    SELECT EXISTS(
        SELECT {{cols}}, COUNT(*)
        FROM {{product}}
        GROUP BY {{cols}}
        HAVING COUNT(*) > 1
    )
    """).render(cols=cols, product=product)

    cur = client.connection.cursor()
    cur.execute(sql)
    output = bool(cur.fetchone()[0])
    cur.close()

    return output


def range_in_column(client, col: str, product):
    """Get range for a column

    Parameters
    ----------
    client
        Database client
    cols
        Column to check
    product
        The relation to check

    Returns
    -------
    tuple
        (minimum, maximum) values
    """
    sql = Template("""
    SELECT MIN({{col}}), MAX({{col}}) FROM {{product}}
    """).render(col=col, product=product)

    cur = client.connection.cursor()
    cur.execute(sql)
    output = cur.fetchone()
    cur.close()

    return output


def exists_row_where(client, criteria: str, product):
    """
    Check whether at least one row exists matching the criteria

    Parameters
    ----------
    client
        Database client
    criteria
        Criteria to evaluate (passed as argument to a WHERE clause)
    product
        The relation to check

    Notes
    -----
    Runs a ``SELECT EXISTS (SELECT * FROM {{product}} WHERE {{criteria}})``
    query

    Returns
    -------
    bool
        True if exists at least one row matching the criteria
    """
    sql = Template("""
    SELECT EXISTS(
        SELECT *
        FROM {{product}}
        WHERE {{criteria}}
    )
    """).render(product=product, criteria=criteria)

    cur = client.connection.cursor()
    cur.execute(sql)
    output = bool(cur.fetchone()[0])
    cur.close()

    return output
