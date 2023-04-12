import pickle
from pathlib import Path

import pandas as pd
import pytest

from ploomber_engine.ipython import PloomberShell
from ploomber import micro


def ones(input_data):
    return pd.Series(input_data)


def twos(ones):
    return ones + 1


def both(ones, twos):
    return pd.DataFrame({"ones": ones, "twos": twos})


def multiply(first, second):
    return first * second


@micro.grid(a=[1, 2], b=[3, 4])
def add(ones, a, b):
    return ones + a + b


@micro.grid(a=[1, 2], b=[3, 4])
@micro.grid(a=[5, 6], b=[7, 8])
def add_many(ones, a, b):
    return ones + a + b


@pytest.mark.parametrize("parallel", [True, False])
def test_inline(tmp_directory, parallel):
    dag = micro.dag_from_functions(
        [ones, twos, both],
        params={"ones": {"input_data": [1] * 3}},
        output="cache",
        parallel=parallel,
    )

    dag.build()

    ones_ = pickle.loads(Path("cache", "ones").read_bytes()).to_dict()
    twos_ = pickle.loads(Path("cache", "twos").read_bytes()).to_dict()
    both_ = pickle.loads(Path("cache", "both").read_bytes()).to_dict()

    assert ones_ == {0: 1, 1: 1, 2: 1}
    assert twos_ == {0: 2, 1: 2, 2: 2}
    assert both_ == {"ones": {0: 1, 1: 1, 2: 1}, "twos": {0: 2, 1: 2, 2: 2}}


@pytest.mark.parametrize(
    "parallel",
    [
        pytest.param(
            True,
            marks=pytest.mark.skip(reason="Gets stuck on GitHub Actions on Python 3.9"),
        ),
        False,
    ],
)
def test_inline_with_manual_dependencies(tmp_directory, parallel):
    dag = micro.dag_from_functions(
        [ones, twos, multiply],
        output="cache",
        params={"ones": {"input_data": [1] * 3}},
        dependencies={"multiply": ["ones", "twos"]},
        parallel=parallel,
    )

    dag.build()

    ones_ = pickle.loads(Path("cache", "ones").read_bytes()).to_dict()
    twos_ = pickle.loads(Path("cache", "twos").read_bytes()).to_dict()
    multiply_ = pickle.loads(Path("cache", "multiply").read_bytes()).to_dict()

    assert ones_ == {0: 1, 1: 1, 2: 1}
    assert twos_ == {0: 2, 1: 2, 2: 2}
    assert multiply_ == {0: 2, 1: 2, 2: 2}


def test_inline_grid(tmp_directory):
    dag = micro.dag_from_functions(
        [ones, add], params={"ones": {"input_data": [1] * 3}}, output="cache"
    )

    dag.build()

    ones_ = pickle.loads(Path("cache", "ones").read_bytes()).to_dict()
    add_0 = pickle.loads(Path("cache", "add-0").read_bytes()).to_dict()
    add_1 = pickle.loads(Path("cache", "add-1").read_bytes()).to_dict()
    add_2 = pickle.loads(Path("cache", "add-2").read_bytes()).to_dict()
    add_3 = pickle.loads(Path("cache", "add-3").read_bytes()).to_dict()

    assert ones_ == {0: 1, 1: 1, 2: 1}
    assert add_0 == {0: 5, 1: 5, 2: 5}
    assert add_1 == {0: 6, 1: 6, 2: 6}
    assert add_2 == {0: 6, 1: 6, 2: 6}
    assert add_3 == {0: 7, 1: 7, 2: 7}


def test_inline_grid_multiple(tmp_directory):
    dag = micro.dag_from_functions(
        [ones, add_many], params={"ones": {"input_data": [1] * 3}}, output="cache"
    )

    dag.build()

    ones_ = pickle.loads(Path("cache", "ones").read_bytes()).to_dict()

    add_many_ = [
        pickle.loads(Path("cache", f"add_many-{i}").read_bytes()).to_dict()
        for i in range(8)
    ]

    assert ones_ == {0: 1, 1: 1, 2: 1}
    assert add_many_ == [
        {0: 13, 1: 13, 2: 13},
        {0: 14, 1: 14, 2: 14},
        {0: 14, 1: 14, 2: 14},
        {0: 15, 1: 15, 2: 15},
        {0: 5, 1: 5, 2: 5},
        {0: 6, 1: 6, 2: 6},
        {0: 6, 1: 6, 2: 6},
        {0: 7, 1: 7, 2: 7},
    ]


def test_root_node_with_no_arguments(tmp_directory):
    def root():
        return 1

    def add(root):
        return root + 1

    dag = micro.dag_from_functions([root, add], hot_reload=False)
    dag.build()

    root_ = pickle.loads(Path("output", "root").read_bytes())
    add_ = pickle.loads(Path("output", "add").read_bytes())

    assert root_ == 1
    assert add_ == 2


def get():
    df = pd.DataFrame({"target": [1, 0, 0, 1], "a": [1, 2, 3, 4]})
    return df


def fn():
    x, y = (  # noqa
        1,
        2,
    )

    i, j = (  # noqa
        1,
        2,
    )


def simple():
    x = 1
    y = 2
    return x, y


def complex():
    for i in range(10):
        for j in range(10):
            if i + j == 2:
                print("hello")


@pytest.mark.parametrize(
    "fn, expected",
    [
        [simple, ["x = 1", "y = 2", "return x, y"]],
        [
            complex,
            [
                "for i in range(10):\n    for j in range(10):\n"
                '        if i + j == 2:\n            print("hello")'
            ],
        ],
    ],
)
@pytest.fixture
def shell():
    yield PloomberShell()
    PloomberShell.clear_instance()


def test_hot_reload(tmp_directory, shell):
    shell.run_cell(
        """
import pandas as pd
from ploomber import micro

def ones(input_data):
    return pd.Series(input_data)

def twos(ones):
    return ones + 1

dag = micro.dag_from_functions(
    [ones, twos],
    params={"ones": {
        "input_data": [1] * 3
    }},
    output='cache',
)
"""
    )

    shell.run_cell("dag.build()")

    shell.run_cell("result = dag.build()")

    assert shell.user_ns["result"]["Ran?"] == [False, False]

    # simulate user re-defines the twos function
    shell.run_cell(
        """
def twos(ones):
    print('new print statement')
    return ones + 1
"""
    )

    shell.run_cell("result = dag.build()")

    # check hot reload is working
    assert shell.user_ns["result"]["name", "Ran?"].to_dict() == {
        "name": ["twos", "ones"],
        "Ran?": [True, False],
    }


@pytest.mark.skip
def test_multi_step_grid(tmp_directory):
    multi_grid = micro.MultiStepGrid()

    @multi_grid(x=[1, 2, 3])
    def first(x):
        return x + 1

    @multi_grid
    def second(first):
        return 2 * first

    # TODO: this isn't good design. if using a multigrid, do we pass
    # multigrid? first? first, second? all?
    dag = micro.dag_from_functions([first, second])
    dag.build()
