import pytest

from ploomber.util import util


def test_mixed_envs():

    # Set a pip dep string
    reqs = """pyflakes==2.4.0\nPygments==2.11.2\n
    pygraphviz @ file:///Users/runner/miniforge3/pygraphviz_1644545996627\n
    PyNaCl==1.5.0\npyparsing==3.0.7"""

    with pytest.warns(UserWarning):
        util.check_mixed_envs(reqs)
