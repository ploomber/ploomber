import warnings
from ploomber.products import _resources


def test_process_resources_warns_on_large_file(monkeypatch):
    import stat

    # this will cause the os.stat call in the _resources module to return 2E+6
    mock_fsize = 2E+6
    mock_fname = "test_resource_file.conf"
    monkeypatch.setattr(_resources.os, 'stat', lambda _: stat)
    mock_fstat_details = _resources.os.stat(__file__)
    mock_fstat_details.st_size = mock_fsize
    monkeypatch.setattr(_resources.os, 'stat', lambda _: mock_fstat_details)

    expected_warn_msg = "File too large. Resource {0}[{1:.2f}MB] > 1MB".format(
        mock_fname, mock_fsize / 1E+6)

    with warnings.catch_warnings(record=True) as w:
        # Cause all warnings to always be triggered.
        warnings.simplefilter("always")
        _resources._check_file_size(mock_fname)
        assert len(w) == 1
        assert issubclass(w[-1].category, UserWarning)
        assert str(w[-1].message) == expected_warn_msg, \
            "Warning is not matching.\nActual warning: " + \
            str(w[-1].message) + "\nExpected warning: " + expected_warn_msg
