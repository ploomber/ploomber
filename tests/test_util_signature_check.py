import pytest
from ploomber.util.util import callback_check, signature_check
from ploomber.exceptions import CallbackSignatureError, TaskRenderError


def test_fn_with_default_values():
    def fn(a, b, default=1):
        pass

    with pytest.raises(CallbackSignatureError):
        callback_check(fn, {'a', 'b'}, allow_default=False)


def test_fn_with_unknown_params():
    def fn(a, b, unknown):
        pass

    with pytest.raises(CallbackSignatureError):
        callback_check(fn, {'a', 'b'})


def test_returns_kwargs_to_use():
    def fn(a, b):
        pass

    assert callback_check(fn, {'a': 1, 'b': 2, 'c': 3}) == {'a': 1, 'b': 2}


def test_signature_check_extra():
    def fn():
        pass

    with pytest.raises(TaskRenderError) as excinfo:
        signature_check(fn, {'b': 1}, 'task')

    error = ('Error rendering task "task" initialized with function "fn". '
             'Got unexpected arguments: [\'b\']')
    assert error == str(excinfo.value)


def test_signature_check_params_missing():
    def fn(a, b):
        pass

    with pytest.raises(TaskRenderError) as excinfo:
        signature_check(fn, {'b': 1}, 'task')

    error = ('Error rendering task "task" initialized with function '
             '"fn". Missing arguments: [\'a\']. '
             'Pass [\'a\'] in "params"')
    assert error == str(excinfo.value)


def test_signature_check_upstream_missing():
    def fn(upstream):
        pass

    with pytest.raises(TaskRenderError) as excinfo:
        signature_check(fn, dict(), 'task')

    error = ('Error rendering task "task" initialized with function '
             '"fn". Missing arguments: [\'upstream\']. '
             'Verify this task declared upstream depedencies or remove the '
             '"upstream" argument from the function')
    assert error == str(excinfo.value)


def test_signature_check_params_and_upstream_missing():
    def fn(upstream, a):
        pass

    with pytest.raises(TaskRenderError) as excinfo:
        signature_check(fn, dict(), 'task')

    error = ('Error rendering task "task" initialized with function "fn". '
             'Missing arguments: [\'a\', \'upstream\']. Verify this '
             'task declared upstream depedencies or remove the "upstream" '
             'argument from the function. '
             'Pass [\'a\'] in "params"')
    assert error == str(excinfo.value)


def test_signature_check_both():
    def fn(a):
        pass

    with pytest.raises(TaskRenderError) as excinfo:
        signature_check(fn, {'b': 1}, 'task')

    error = ('Error rendering task "task" initialized with function '
             '"fn". Got unexpected arguments: [\'b\']. '
             'Missing arguments: [\'a\']. Pass [\'a\'] in "params"')

    assert error == str(excinfo.value)


def test_signature_check_ignores_params_w_default():
    def fn(a, b=1):
        pass

    assert signature_check(fn, {'a': 1}, 'task')
