"""
A pipeline that dumps and processes rows since last execution
"""

# +
import shutil
from pathlib import Path
import tempfile
import pandas as pd

from sqlalchemy import create_engine

from ploomber import DAG
from ploomber.tasks import SQLDump, PythonCallable
from ploomber.products import File
from ploomber.clients import SQLAlchemyClient


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
    # just save the new value
    if 'last_value' not in metadata:
        metadata['last_value'] = new_max
        return metadata
    # we have a previously saved value, verify it's bigger than the one we saw
    # last time...
    if new_max > metadata['last_value']:
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


def make(tmp):
    """Make the dag
    """
    tmp = Path(tmp)

    dag = DAG()
    client = SQLAlchemyClient('sqlite:///' + str(tmp / 'my_db.db'))

    dag.clients[SQLDump] = client

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

    plus_one = PythonCallable(_plus_one, File(tmp / 'plus_one.csv'),
                              dag=dag, name='plus_one')

    dump >> plus_one

    return dag


# ## Testing

# create dag
tmp = tempfile.mkdtemp()
dag = make(tmp)


# add some sample data to the database
engine = create_engine('sqlite:///' + str(Path(tmp, 'my_db.db')))
df = pd.DataFrame({'x': range(10)})
df.to_sql('data', engine)

# run dag, should pull this first 10 observations
dag.build(force=True)

# checking downloaded data with the plus one added
df = pd.read_csv(str(dag['plus_one']))
assert df.x.min() == 1 and df.shape[0] == 10
df

# run the dag again, this time plus one should be empty as there
# are no new rows and we are forcing a run
dag.build(force=True)
df = pd.read_csv(str(dag['plus_one']))
assert not df.shape[0]

# simulate new data arrival
df = pd.DataFrame({'x': range(10)})
df['x'] = df.x + 10
df.to_sql('data', engine, if_exists='append')

# +
# should only get new rows
dag.build(force=True)
df = pd.read_csv(str(dag['plus_one']))
assert df.x.min() == 11

df
# -


shutil.rmtree(tmp)
