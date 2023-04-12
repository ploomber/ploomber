from pathlib import Path

import pytest

from ploomber.spec import DAGSpec


@pytest.fixture
def tmp_spec(tmp_directory):
    Path("script.py").write_text(
        """
from pathlib import Path

# %% tags=["parameters"]
upstream = None

# %%
Path(product).touch()
"""
    )

    spec = {
        "tasks": [
            {
                "source": "script.py",
                "product": "file.txt",
                "class": "ScriptRunner",
            }
        ]
    }

    return spec


def test_spec_with_scriptrunner(tmp_spec):
    dag = DAGSpec(tmp_spec).to_dag()
    dag.build()
