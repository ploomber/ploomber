import pickle
from pathlib import Path

import pandas as pd
import pytest

from ploomber import inline


def ones(input_data):
    return pd.Series(input_data)


def twos(ones):
    return ones + 1


def both(ones, twos):
    return pd.DataFrame({"ones": ones, "twos": twos})


def multiply(first, second):
    return first * second


@pytest.mark.parametrize('parallel', [True, False])
def test_inline(tmp_directory, parallel):
    dag = inline.dag_from_functions(
        [ones, twos, both],
        params={"ones": {
            "input_data": [1] * 3
        }},
        output='cache',
        parallel=parallel,
    )

    dag.build()

    ones_ = pickle.loads(Path('cache', 'ones').read_bytes()).to_dict()
    twos_ = pickle.loads(Path('cache', 'twos').read_bytes()).to_dict()
    both_ = pickle.loads(Path('cache', 'both').read_bytes()).to_dict()

    assert ones_ == {0: 1, 1: 1, 2: 1}
    assert twos_ == {0: 2, 1: 2, 2: 2}
    assert both_ == {'ones': {0: 1, 1: 1, 2: 1}, 'twos': {0: 2, 1: 2, 2: 2}}


def test_inline_with_manual_dependencies(tmp_directory):
    dag = inline.dag_from_functions(
        [ones, twos, multiply],
        output="cache",
        params={"ones": {
            "input_data": [1] * 3
        }},
        dependencies={"multiply": ["ones", "twos"]},
        parallel=True,
    )

    dag.build()

    ones_ = pickle.loads(Path('cache', 'ones').read_bytes()).to_dict()
    twos_ = pickle.loads(Path('cache', 'twos').read_bytes()).to_dict()
    multiply_ = pickle.loads(Path('cache', 'multiply').read_bytes()).to_dict()

    assert ones_ == {0: 1, 1: 1, 2: 1}
    assert twos_ == {0: 2, 1: 2, 2: 2}
    assert multiply_ == {0: 2, 1: 2, 2: 2}
