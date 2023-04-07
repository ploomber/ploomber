import pickle
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import matplotlib.pyplot as plt
import nbformat
import pytest

from ploomber_engine.ipython import PloomberShell
from ploomber.exceptions import DAGBuildError, DAGRenderError
from ploomber import micro
from ploomber.micro import _capture


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


@micro.capture
def plot_ones(ones):
    # tag=plot
    plt.plot(ones)
    x = 1
    return x


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


@pytest.mark.skip(reason="this test hangs up intermittently")
@pytest.mark.parametrize("parallel", [True, False])
@pytest.mark.parametrize("debug", [None, "now", "later"])
def test_capture(tmp_directory, parallel, debug):
    dag = micro.dag_from_functions(
        [ones, plot_ones],
        params={"ones": {"input_data": [1] * 3}},
        output="cache",
        parallel=parallel,
    )

    dag.build(debug=debug)

    ones_ = pickle.loads(Path("cache", "ones").read_bytes()).to_dict()
    plot_ones_ = pickle.loads(Path("cache", "plot_ones").read_bytes())

    assert ones_ == {0: 1, 1: 1, 2: 1}
    assert plot_ones_ == 1
    assert Path("cache", "plot_ones.html").is_file()
    nb = nbformat.reads(
        Path("cache", "plot_ones.ipynb").read_text(), as_version=nbformat.NO_CONVERT
    )
    assert nb.cells[0].metadata.tags[0] == "plot"


# this fails since it tries to unpickle the HTML
@pytest.mark.xfail
def test_capture_can_return_nothing(tmp_directory):
    @micro.capture
    def first():
        x = 1
        return x

    # end nodes don't have to return anything
    @micro.capture
    def second(first):
        pass

    dag = micro.dag_from_functions([first, second])
    dag.build()

@pytest.mark.xfail(reason="_capture module has been deprecated")
def test_capture_debug_now(tmp_directory, monkeypatch):
    @micro.capture
    def number():
        x, y = 1, 0
        x / y

    dag = micro.dag_from_functions([number], hot_reload=False)

    class MyException(Exception):
        pass

    mock = Mock(side_effect=MyException)
    monkeypatch.setattr(_capture, "debug_if_exception", mock)

    with pytest.raises(MyException):
        dag.build(debug="now")

    callable_ = mock.call_args[1]["callable_"]
    task_name = mock.call_args[1]["task_name"]

    with pytest.raises(ZeroDivisionError):
        callable_()

    assert task_name == "number"

@pytest.mark.xfail(reason="_capture module has been deprecated")
def test_capture_debug_later(tmp_directory, monkeypatch):
    @micro.capture
    def number():
        x, y = 1, 0
        x / y

    dag = micro.dag_from_functions([number], hot_reload=False)

    with pytest.raises(DAGBuildError):
        dag.build(debug="later")

    assert Path("number.dump").is_file()

@pytest.mark.xfail(reason="_capture module has been deprecated")
def test_capture_that_depends_on_capture(tmp_directory):
    @micro.capture
    def first():
        x = 1
        return x

    @micro.capture
    def second(first):
        second = first + 1
        return second

    dag = micro.dag_from_functions([first, second], hot_reload=False)
    dag.build()


@pytest.mark.xfail
def test_error_when_returning_non_variable_and_capture(tmp_directory):
    @micro.capture
    def first():
        return 1

    dag = micro.dag_from_functions([first])
    dag.build(debug="now")


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


# TODO: also test with grid
def test_decorated_root_with_input_data(tmp_directory):

    # this is failing because it's not passing input_data - we're not
    # validating the signature,and just modifying the user's namespace,
    # but we have to, otherwise the error is confusing: "input_data" is not
    # defined
    @micro.capture
    def root(input_data):
        x = input_data + 1
        return x

    dag = micro.dag_from_functions([root], hot_reload=False)

    with pytest.raises(DAGRenderError) as excinfo:
        dag.build()

    assert "'root' function missing 'input_data'" in str(excinfo.value)


# TODO: also try with grid
# NOTE: this is failing because it's trying to unpickle the html
def test_decorated_root_without_arguments(tmp_directory):
    @micro.capture
    def root():
        x = 1
        return x

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


@micro.capture
@micro.grid(
    model=[
        "RandomForestClassifier",
        "AdaBoostClassifier",
        "ExtraTreesClassifier",
    ]
)
def fit(get, model):
    _ = get.drop("target", axis="columns")
    _ = get.target

    # tag=plot
    plt.plot([1, 2, 3])

    return model

@pytest.mark.xfail(reason="_capture module has been deprecated")
def test_decorated_with_capture_and_grid(tmp_directory):
    dag = micro.dag_from_functions([get, fit])
    dag.build()

    nb = nbformat.reads(
        Path("output", "fit-0.ipynb").read_text(), as_version=nbformat.NO_CONVERT
    )
    assert nb.cells[-2].metadata.tags[0] == "plot"


def fn():
    x, y = (  # noqa
        1,
        2,
    )

    i, j = (  # noqa
        1,
        2,
    )

@pytest.mark.xfail(reason="_capture module has been deprecated")
def test_get_body_statements():
    assert len(_capture._get_body_statements(fn)) == 2


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

@pytest.mark.xfail(reason="_capture module has been deprecated")
def test_deindents_statements(fn, expected):
    assert _capture._get_body_statements(fn) == expected

@pytest.mark.xfail(reason="_capture module has been deprecated")
@pytest.mark.parametrize(
    "source, expected",
    [
        ["# tag=plot", "plot"],
        ["# tag=cool_plot", "cool_plot"],
        ["# tag=cool-plot", "cool-plot"],
        ["# tag=plot0", "plot0"],
        ["# tag=0plot", "0plot"],
        ["\n    # tag=plot\n    plot.confusion_matrix(y_test, y_pred)\n", "plot"],
    ],
)
def test_parse_tag(source, expected):
    assert _capture._parse_tag(source) == expected


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
