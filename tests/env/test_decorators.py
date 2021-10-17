from ploomber.env.decorators import with_env
import pytest


def test_one_arg_not_start_with_env():
    @with_env({'a': 1})
    def works(env, b):
        return b

    assert works(2) == (2)


def test_many_args_not_start_with_env():
    @with_env({'a': 1})
    def works(env, b, c):
        return b, c

    assert works(2, 3) == (2, 3)


def test_one_arg_starting_with_env():
    with pytest.raises(RuntimeError) as excinfo:

        @with_env({'a': 1})
        def does_not_work(env, env_something):
            return env_something

    assert 'has arguments starting with "env"' in str(excinfo.value)


def test_many_args_starting_with_env():
    with pytest.raises(RuntimeError) as excinfo:

        @with_env({'a': 1})
        def does_not_work(env, env_a, env_b):
            return env_a, env_b

    assert 'has arguments starting with "env"' in str(excinfo.value)


def test_cap_arg_starting_with_env():
    with pytest.raises(RuntimeError) as excinfo:

        @with_env({'a': 1})
        def does_not_work(env, ENV_something):
            return ENV_something

    assert 'has arguments starting with "env"' in str(excinfo.value)
