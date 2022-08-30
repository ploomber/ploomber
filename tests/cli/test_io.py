from ploomber.cli.io import command_endpoint
import pytest


def test_command_endpoint_shows_full_traceback(capsys):

    @command_endpoint
    def non_subclass_exception():
        try:
            raise ValueError("error 1")
        except ValueError:
            try:
                raise ValueError("error 2")
            except ValueError:
                raise ValueError("error 3")

    with pytest.raises(SystemExit) as excinfo:
        non_subclass_exception()
    captured = capsys.readouterr()

    # check that the full traceback is displayed
    assert "error 1" in captured.err
    assert "error 2" in captured.err
    assert "error 3" in captured.err
    assert excinfo.value.code == 1
