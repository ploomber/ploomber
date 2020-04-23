"""
@with_env and @load_env facilitate loading env.yaml files
"""
from pathlib import Path

from ploomber.tasks import PythonCallable
from ploomber.products import File
from ploomber import DAG, with_env, load_env


@with_env({'path': {'data': '/tmp/'},
           'db_uri': 'sqlite:///my_db.db'})
# the first parameter of the decorated function must be named "env", will
# be automatically passed to the function
def dag_factory(env):
    dag = DAG()
    write = PythonCallable(_write_file,
                           File(env.path.data / 'file.txt'),
                           dag=dag,
                           name='write_file')

    # if you want to defer task creation to a task factory you can do so
    # by adding a function and decorating ig with @load_env
    another = write_another(dag)

    write >> another

    return dag


def _write_file(product):
    Path(str(product)).write_text('This is some text')


# load env does not create a new Env, it only loads the existing one
@load_env
def write_another(env, dag):
    return PythonCallable(_write_another,
                          File(env.path.data / 'another.txt'),
                          dag=dag,
                          name='write_another')


def _write_another(upstream, product):
    txt = Path(str(upstream['write_file'])).read_text()
    Path(str(product)).write_text(txt + '. And even more text.')


dag = dag_factory()
dag.status()
