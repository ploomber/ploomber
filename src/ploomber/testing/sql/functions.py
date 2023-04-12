"""
Function for testing testing SQL relations
"""
from typing import Union, List

from jinja2 import Template


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
    # NOTE: SELECT EXISTS does not work on oracle
    # it can be SELECT 1 FROM EXISTS(...) dual (dual is a system table
    # it always exists). Should we support it?
    sql = Template(
        """
    SELECT EXISTS(
        SELECT * FROM {{product}}
        WHERE {{cols | join(' is null or ') }} is null
    )
    """
    ).render(cols=cols, product=product)

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

    sql = Template(
        """
    SELECT DISTINCT {{col}} FROM {{product}}
    """
    ).render(col=col, product=product)

    cur = client.connection.cursor()
    cur.execute(sql)
    output = cur.fetchall()
    cur.close()

    return set(o[0] for o in output)


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
    sql = Template(
        """
    SELECT MIN({{col}}), MAX({{col}}) FROM {{product}}
    """
    ).render(col=col, product=product)

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
    sql = Template(
        """
    SELECT EXISTS(
        SELECT *
        FROM {{product}}
        WHERE {{criteria}}
    )
    """
    ).render(product=product, criteria=criteria)

    cur = client.connection.cursor()
    cur.execute(sql)
    output = bool(cur.fetchone()[0])
    cur.close()

    return output
