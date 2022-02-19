from functools import partial

from papermill.exceptions import PapermillExecutionError

from ploomber.executors import _format
from ploomber.exceptions import TaskBuildError, RenderError, TaskRenderError

# TODO: test with SQLTaskBuildError


def fn():
    raise ValueError('some error')


def fn_pm():
    raise PapermillExecutionError(0, 0, 'source', 'ename', 'evalue', 't')


def fn_task_render():
    raise TaskRenderError('some error')


def fn_render():
    raise RenderError('some error happened')


def chain(fn, exc):
    try:
        fn()
    except Exception as e:
        raise exc from e


def test_exception_two_nested_task_build():
    fn_ = partial(chain, fn=fn, exc=TaskBuildError('more info'))

    try:
        chain(fn_, TaskBuildError('even more info'))
    except Exception as exc:
        res = _format.exception(exc)

    expected = ('\nValueError: some error\n\n'
                'ploomber.exceptions.TaskBuildError: more info'
                '\nploomber.exceptions.TaskBuildError: even more info')
    assert expected in res
    assert "raise ValueError('some error')" in res


def test_exception_one_task_build():
    fn_ = partial(chain, fn=fn, exc=ValueError('more info'))

    try:
        chain(fn_, TaskBuildError('even more info'))
    except Exception as exc:
        res = _format.exception(exc)

    expected = ('\nValueError: more info\n\n'
                'ploomber.exceptions.TaskBuildError: even more info')
    assert expected in res
    assert 'raise exc from e' in res
    assert "raise ValueError('some error')" in res


def test_exception_no_task_build():
    fn_ = partial(chain, fn=fn, exc=ValueError('more info'))

    try:
        chain(fn_, ValueError('even more info'))
    except Exception as exc:
        res = _format.exception(exc)

    assert ('raise ValueError(\'some error\')\nValueError: some error\n'
            in res)
    assert 'raise exc from e\nValueError: more info\n' in res
    assert 'raise exc from e\nValueError: even more info\n' in res


def test_exception_papermill_error():
    fn_ = partial(chain, fn=fn_pm, exc=TaskBuildError('more info'))

    try:
        chain(fn_, TaskBuildError('even more info'))
    except Exception as exc:
        res = _format.exception(exc)

    assert 'Traceback (most recent call last):' not in res


def test_exception_render_error():
    fn_ = partial(chain, fn=fn_render, exc=TaskBuildError('more info'))

    try:
        chain(fn_, TaskBuildError('even more info'))
    except Exception as exc:
        res = _format.exception(exc)

    assert 'Traceback (most recent call last):' not in res


def test_exception_task_render_error():
    fn_ = partial(chain, fn=fn_task_render, exc=TaskBuildError('more info'))

    try:
        chain(fn_, TaskBuildError('even more info'))
    except Exception as exc:
        res = _format.exception(exc)

    assert 'Traceback (most recent call last):' not in res
