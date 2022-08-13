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


@inline.grid(a=[1, 2], b=[3, 4])
def add(ones, a, b):
    return ones + a + b


@inline.grid(a=[1, 2], b=[3, 4])
@inline.grid(a=[5, 6], b=[7, 8])
def add_many(ones, a, b):
    return ones + a + b


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


def test_inline_grid(tmp_directory):
    dag = inline.dag_from_functions([ones, add],
                                    params={"ones": {
                                        "input_data": [1] * 3
                                    }},
                                    output='cache')

    dag.build()

    ones_ = pickle.loads(Path('cache', 'ones').read_bytes()).to_dict()
    add_0 = pickle.loads(Path('cache', 'add-0').read_bytes()).to_dict()
    add_1 = pickle.loads(Path('cache', 'add-1').read_bytes()).to_dict()
    add_2 = pickle.loads(Path('cache', 'add-2').read_bytes()).to_dict()
    add_3 = pickle.loads(Path('cache', 'add-3').read_bytes()).to_dict()

    assert ones_ == {0: 1, 1: 1, 2: 1}
    assert add_0 == {0: 5, 1: 5, 2: 5}
    assert add_1 == {0: 6, 1: 6, 2: 6}
    assert add_2 == {0: 6, 1: 6, 2: 6}
    assert add_3 == {0: 7, 1: 7, 2: 7}


def test_inline_grid_multiple(tmp_directory):
    dag = inline.dag_from_functions([ones, add_many],
                                    params={"ones": {
                                        "input_data": [1] * 3
                                    }},
                                    output='cache')

    dag.build()

    ones_ = pickle.loads(Path('cache', 'ones').read_bytes()).to_dict()

    add_many_ = [
        pickle.loads(Path('cache', f'add_many-{i}').read_bytes()).to_dict()
        for i in range(8)
    ]

    assert ones_ == {0: 1, 1: 1, 2: 1}
    assert add_many_ == [{
        0: 13,
        1: 13,
        2: 13
    }, {
        0: 14,
        1: 14,
        2: 14
    }, {
        0: 14,
        1: 14,
        2: 14
    }, {
        0: 15,
        1: 15,
        2: 15
    }, {
        0: 5,
        1: 5,
        2: 5
    }, {
        0: 6,
        1: 6,
        2: 6
    }, {
        0: 6,
        1: 6,
        2: 6
    }, {
        0: 7,
        1: 7,
        2: 7
    }]
