"""
SourceLoader
============

How to manage non-Python source files using SourceLoader, this examples shows
how to load parametrized SQL scripts and make use of advanced jinja2 features
"""

###############################################################################
# Data pipelines code usually includes non-Python files (e.g. SQL scripts),
# SourceLoader provides a convenient way of loading them to avoid hardcoding
# paths to files and enables the use of advanced jinja2 features such as
# macros.

from pathlib import Path
import tempfile

import pandas as pd

from ploomber.clients import SQLAlchemyClient
from ploomber.tasks import SQLTransfer, SQLScript
from ploomber.products import SQLiteRelation
from ploomber import DAG, SourceLoader


###############################################################################
# We first setup our sample environment, a sqlite database with some data

tmp_dir = Path(tempfile.mkdtemp())
client = SQLAlchemyClient('sqlite:///' + str(tmp_dir / 'my_db.db'))
df = pd.DataFrame({'x': range(10)})
df.to_sql('data', client.engine)

###############################################################################
# We now simulate our code environment: a folder with SQL scripts, for
# simplicity, we are saving them in the same location as the data but in a real
# project we should keep and data separate

_ = Path(tmp_dir, 'data_select.sql').write_text('SELECT * FROM data')

###############################################################################
# Unde the hood SourceLoader initializes a jinja2.Environment which allows us
# to use features such as macros
Path(tmp_dir, 'macros.sql').write_text("""
{% macro my_macro() -%}
    -- This is a macro
{%- endmacro %}
""")

_ = (Path(tmp_dir, 'subset_create.sql')
     .write_text("""
{% from 'macros.sql' import my_macro %}

{{my_macro()}}

CREATE TABLE {{product}} AS
SELECT * FROM
{{upstream["transfer"]}} WHERE x > 1
"""))

###############################################################################
# DAG declaration

dag = DAG()
dag.clients[SQLTransfer] = client
dag.clients[SQLiteRelation] = client
dag.clients[SQLScript] = client

source_loader = SourceLoader(tmp_dir)

transfer = SQLTransfer(source_loader['data_select.sql'],
                       product=SQLiteRelation((None, 'data2', 'table')),
                       dag=dag,
                       name='transfer')

subset = SQLScript(source_loader['subset_create.sql'],
                   product=SQLiteRelation((None, 'subset', 'table')),
                   dag=dag,
                   name='subset')

transfer >> subset

dag.render()

###############################################################################
# Our macro is correctly rendered:

print(dag['subset'].source_code)


###############################################################################
# Plot and execute pipeline:

dag.plot(output='matplotlib')

dag.build()
