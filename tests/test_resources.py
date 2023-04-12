import stat

import pytest

from ploomber.products import _resources


def test_process_resources_warns_on_large_file(monkeypatch):
    # this will cause the os.stat call in the _resources module to return 2E+6
    mock_fsize = 2e6
    mock_fname = "test_resource_file.conf"
    monkeypatch.setattr(_resources.os, "stat", lambda _: stat)
    mock_fstat_details = _resources.os.stat(__file__)
    mock_fstat_details.st_size = mock_fsize
    monkeypatch.setattr(_resources.os, "stat", lambda _: mock_fstat_details)

    with pytest.warns(UserWarning) as record:
        _resources._check_file_size(mock_fname)

    assert len(record) == 1
    assert "It is not recommended to use large files" in record[0].message.args[0]
