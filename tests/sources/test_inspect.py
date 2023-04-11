from pathlib import Path

import pytest

from test_pkg.decorated.functions import (
    decorated_function,
    function,
    double_decorated_function,
)
from ploomber.sources import inspect


@pytest.mark.parametrize(
    "fn",
    [
        function,
        decorated_function,
        double_decorated_function,
    ],
)
def test_getfile_from_wrapped_function(fn):
    assert Path(inspect.getfile(fn)).name == "functions.py"
