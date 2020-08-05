from argparse import ArgumentParser
from ploomber.cli.parsers import _add_args_from_callable


def fn(a: int, b: float, c: str, d: bool, e):
    pass


def test_add_type_to_arg_parser():
    parser = ArgumentParser()

    _add_args_from_callable(parser, fn)

    actions = {action.dest: action for action in parser._actions}

    assert actions['a'].type is int
    assert actions['b'].type is float
    assert actions['c'].type is str
    assert actions['d'].type is bool
