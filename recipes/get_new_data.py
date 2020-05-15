"""
A pipeline that dumps and processes rows since last execution
"""

# +
import logging
import shutil
from pathlib import Path
import tempfile
import pandas as pd

from sqlalchemy import create_engine

from ploomber import DAG
from ploomber.tasks import SQLDump, PythonCallable, SQLUpload
from ploomber.products import File, SQLiteRelation
from ploomber.clients import SQLAlchemyClient
from ploomber.exceptions import DAGBuildEarlyStop
from ploomber.executors import Serial


# -

def add_last_value(metadata, product):
    """Hook to save last value downloaded before saving metadata
    """
    df = pd.read_csv(str(product))

    # data has not been changed (the query triggered downloading 0 rows),
    # do not alter metadata
    if not df.shape[0]:
        return metadata

    # got some data...

    new_max = int(df.x.max())

    # there is no last_value in metadata, this is the first time we run this,
    # just save the new value or if
    # we have a previously saved value, verify it's bigger than the one we saw
    # last time...
    if ('last_value' not in metadata
            or new_max > metadata['last_value']):
        metadata['last_value'] = new_max
        return metadata

    # NOTE: the other scenarios should not happen, our query
    # has a strict inequality


def _plus_one(product, upstream):
    """A function that adds 1 to column x
    """
    df = pd.read_csv(str(upstream['dump']))
    df['x'] = df.x + 1
    df.to_csv(str(product), index=False)


def dump_on_finish(product):
    df = pd.read_csv(str(product))

    if not df.shape[0]:
        raise DAGBuildEarlyStop('No new observations')


def make(tmp):
    """Make the dag
    """
    tmp = Path(tmp)

    executor = Serial(build_in_subprocess=False, catch_exceptions=True)
    dag = DAG(executor=executor)
    client_source = SQLAlchemyClient('sqlite:///' + str(tmp / 'source.db'))
    client_target = SQLAlchemyClient('sqlite:///' + str(tmp / 'target.db'))

    dag.clients[SQLDump] = client_source
    dag.clients[SQLUpload] = client_target
    dag.clients[SQLiteRelation] = client_target

    out = File(tmp / 'x.csv')
    out.pre_save_metadata = add_last_value

    dump = SQLDump("""
    {% if product.metadata.timestamp %}
        SELECT * FROM data
        WHERE x > {{product.metadata['last_value']}}
    {% else %}
        SELECT * FROM  data
    {% endif %}
    """, out, dag=dag, name='dump', chunksize=None)
    dump.on_finish = dump_on_finish

    plus_one = PythonCallable(_plus_one, File(tmp / 'plus_one.csv'),
                              dag=dag, name='plus_one')

    upload = SQLUpload('{{upstream["plus_one"]}}',
                       product=SQLiteRelation((None, 'plus_one', 'table')),
                       dag=dag,
                       name='upload',
                       to_sql_kwargs={'if_exists': 'append', 'index': False})

    dump >> plus_one >> upload

    return dag


# ## Testing
logging.basicConfig(level=logging.INFO)

# create dag
tmp = tempfile.mkdtemp()
dag = make(tmp)


# add some sample data to the database
engine = create_engine('sqlite:///' + str(Path(tmp, 'source.db')))
df = pd.DataFrame({'x': range(10)})
df.to_sql('data', engine)

target = create_engine('sqlite:///' + str(Path(tmp, 'target.db')))

# run dag, should pull this first 10 observations
dag.build(force=True)

# checking downloaded data with the plus one added
df = pd.read_sql('SELECT * FROM plus_one', target)
assert df.x.max() == 10 and df.shape[0] == 10
df

# run the dag again, this time plus one should be the same
dag.build(force=True)

df = pd.read_sql('SELECT * FROM plus_one', target)
assert df.x.max() == 10 and df.shape[0] == 10
df


# simulate new data arrival
df = pd.DataFrame({'x': range(10)})
df['x'] = df.x + 10
df.to_sql('data', engine, if_exists='append')

# +
# should only get new rows
dag.build(force=True)

df = pd.read_sql('SELECT * FROM plus_one', target)
assert df.x.max() == 20 and df.shape[0] == 20
df
# -


shutil.rmtree(tmp)
