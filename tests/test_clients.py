from pathlib import Path

from ploomber import DAG
from ploomber.tasks import ShellScript
from ploomber.products import File
from ploomber.clients import ShellClient


def test_shell_client(tmp_directory):
    path = Path(tmp_directory, 'a_file')

    client = ShellClient()
    code = """
    touch a_file
    """
    assert not path.exists()

    client.execute(code)

    assert path.exists()


def test_shell_client_with_custom_template(tmp_directory):
    path = Path(tmp_directory, 'a_file')

    client = ShellClient(run_template='ruby {{path_to_code}}')
    code = """
    require 'fileutils'
    FileUtils.touch "a_file"
    """
    assert not path.exists()

    client.execute(code)

    assert path.exists()


def test_custom_client_in_dag(tmp_directory):
    path = Path(tmp_directory, 'a_file')

    dag = DAG()

    client = ShellClient(run_template='ruby {{path_to_code}}')

    dag.clients[ShellScript] = client

    ShellScript("""
    require 'fileutils'
    FileUtils.touch "{{product}}"
    """,
                product=File(path),
                dag=dag,
                name='ruby_script')

    assert not path.exists()

    dag.build()

    assert path.exists()
