# ## Intermediate example
#
# This example shows some other features when building a `DAG`.
#
# Note: run this using `ipython` or in a Jupyter notebook (it won't run using `python`).

# +
import logging

from ploomber.products import File, PostgresRelation
from ploomber.tasks import (BashCommand, PythonCallable,
                                    SQLScript)
from ploomber.dag import DAG
from ploomber.clients import SQLAlchemyClient
from ploomber import testing
from ploomber import Env

from IPython.display import Image, display
# -

from train import train_and_save_report
import util
from download_dataset import download_dataset
from sample import sample


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


env = Env()
home = env.path.home
path_to_sample = env.path.input / 'sample'

(env.path.input / 'raw').mkdir(exist_ok=True, parents=True)
path_to_sample.mkdir(exist_ok=True)

# +
uri = util.load_db_uri()

# for tasks that execute in remote systems, you have to provide a client,
# a SQLAlchemyClient is a wrap around a sql alchemy engine that helps
# commmunicate with databases
pg_client = SQLAlchemyClient(uri)

# +
dag = DAG()

# Tasks and Tables/Views accept a client in the constructor, but you
# can also declared them using dag.clients to avoid passing them
# every time
dag.clients[PostgresRelation] = pg_client
dag.clients[SQLScript] = pg_client
# -

# calling delete on a DAG will cause all its products to be deleted
# let's clean up our databse by running these
dag.product.delete()


# let's introduce a new Task: bash command, this command will generate
# 3 products, it is recommended for Tasks to have a single Product
# but you can add more if needed
get_data = BashCommand((home / 'get_data.sh').read_text(),
                       (File(env.path.input / 'raw' / 'red.csv'),
                        File(env.path.input / 'raw' / 'white.csv'),
                        File(env.path.input / 'raw' / 'names')),
                       dag,
                       name='get_data')

sample = PythonCallable(sample,
                        (File(env.path.input / 'sample' / 'red.csv'),
                         File(env.path.input / 'sample' / 'white.csv')),
                        name='sample',
                        dag=dag)
get_data >> sample

# Tasks can have parameters, on this case we are passing a uri parameter to the csvsql command line
# utility, furthermore, since we declared a product and an upstream dependency, those are
# available as parameters as well. This task does not create a File but a PostgresRelation,
# this type of product is a a table named red in the public schema
red_task = BashCommand(('csvsql --db {{uri}} --tables {{product.name}} --insert {{upstream["sample"][0]}} '
                        '--overwrite'),
                       PostgresRelation(('public', 'red', 'table')),
                       dag,
                       params=dict(uri=uri),
                       split_source_code=False,
                       name='red')
sample >> red_task

white_task = BashCommand(('csvsql --db {{uri}} --tables {{product.name}} --insert {{upstream["sample"][1]}} '
                          '--overwrite'),
                         PostgresRelation(('public', 'white', 'table')),
                         dag,
                         params=dict(uri=uri),
                         name='white')
sample >> white_task


# let's introduce a new type of task, a SQLScript, this task will execute
# a script in a SQL database (any database supported by sql alchemy)
wine_task = SQLScript(home / 'sql' / 'create_wine.sql',
                      PostgresRelation(('public', 'wine', 'table')),
                      dag, name='wine')
(red_task + white_task) >> wine_task


dataset_task = SQLScript(home / 'sql' / 'create_dataset.sql',
                         PostgresRelation(('public', 'dataset', 'table')),
                         dag, name='dataset')
wine_task >> dataset_task


training_task = SQLScript(home / 'sql' / 'create_training.sql',
                          PostgresRelation(('public', 'training', 'table')),
                          dag, name='training')
dataset_task >> training_task


testing_table = PostgresRelation(('public', 'testing', 'table'))
# testing_table.tests = [testing.Postgres.no_nas_in_column('label')]
testing_task = SQLScript(home / 'sql' / 'create_testing.sql',
                         testing_table, dag, name='testing')

dataset_task >> testing_task


path_to_dataset = env.path.input / 'datasets'
params = dict(path_to_dataset=path_to_dataset)
download_task = PythonCallable(download_dataset,
                               (File(path_to_dataset / 'training.csv'),
                                File(path_to_dataset / 'testing.csv')),
                               dag, params=params, name='download')
training_task >> download_task
testing_task >> download_task


path_to_report = env.path.input / 'reports' / 'latest.txt'
params = dict(path_to_dataset=path_to_dataset,
              path_to_report=path_to_report)
train_task = PythonCallable(train_and_save_report, File(
    path_to_report), dag, params=params, name='fit')
download_task >> train_task

dag.status()

stats = dag.build()

stats
