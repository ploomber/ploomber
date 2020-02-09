from pathlib import Path

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
