"""
Testing SQL relations
"""
from jinja2 import Template


def nulls_in_columns(client, cols, product):
    """Check if any column has NULL values, returns bool
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


def distinct_values_in_column(client, col, product):
    """Get distinct values in a column, returns a set
    """
    sql = Template("""
    SELECT DISTINCT {{col}} FROM {{product}}
    """).render(col=col, product=product)

    cur = client.connection.cursor()
    cur.execute(sql)
    output = cur.fetchall()
    cur.close()

    return set(o[0] for o in output)


def duplicates_in_column(client, col, product):
    """Check if a column has duplicated values, returns bool
    """
    sql = Template("""
    SELECT EXISTS(
        SELECT {{col}}, COUNT(*)
        FROM {{product}}
        GROUP BY {{col}}
        HAVING COUNT(*) > 1
    )
    """).render(col=col, product=product)

    cur = client.connection.cursor()
    cur.execute(sql)
    output = bool(cur.fetchone()[0])
    cur.close()

    return output


def range_in_column(client, col, product):
    """Get range for a column, returns a (min_value, max_value) tuple
    """
    sql = Template("""
    SELECT MIN({{col}}), MAX({{col}}) FROM {{product}}
    """).render(col=col, product=product)

    cur = client.connection.cursor()
    cur.execute(sql)
    output = cur.fetchone()
    cur.close()

    return output
