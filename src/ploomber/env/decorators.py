import types
from functools import wraps
from inspect import getfullargspec

from ploomber.env.env import Env
from ploomber.env.EnvDict import EnvDict


def _validate_env_decorated_fn(fn):
    spec = getfullargspec(fn)
    args_fn = spec.args

    if not len(args_fn):
        raise RuntimeError('Function "{}" does not take arguments, '
                           '@with_env decorated functions should '
                           'have env as their first artgument'
                           .format(fn.__name__))

    if args_fn[0] != 'env':
        raise RuntimeError('Function "{}" does not "env" as its first '
                           'argument, which is required to use the '
                           '@with_env decorator'
                           .format(fn.__name__))

    # TODO: check no arg in the function starts with env (other than env)


def load_env(fn):
    """
    A function decorated with @load_env will be called with the current
    environment in an env keyword argument
    """
    _validate_env_decorated_fn(fn)

    @wraps(fn)
    def wrapper(*args, **kwargs):
        return fn(Env(), *args, **kwargs)

    return wrapper


def with_env(source):
    """
    A function decorated with @with_env that starts and environment during
    the execution of a function.

    Notes
    -----
    The first argument of a function decorated with @with_env must be named
    "env"

    You can replace values in the environment, e.g. if you want to replace
    env.key.another, you can call the decorated function with:
    my_fn(env__key__another='my_new_value')

    The environment is resolved at import time, changes to the working
    directory will not affect initializaiton

    Examples
    --------
    .. literalinclude:: ../examples/short/with_env.py

    """
    def decorator(fn):
        _validate_env_decorated_fn(fn)
        env_dict = EnvDict(source)
        fn._env_dict = env_dict

        @wraps(fn)
        def wrapper(*args, **kwargs):
            to_replace = {k: v for k, v in kwargs.items()
                          if k.startswith('env__')}

            for key in to_replace.keys():
                kwargs.pop(key)

            env = Env.start(env_dict)

            for key, new_value in to_replace.items():
                elements = key.split('__')
                to_edit = env._data._data

                for e in elements[1:-1]:
                    to_edit = to_edit[e]

                if to_edit.get(elements[-1]) is None:
                    Env.end()
                    dotted_path = '.'.join(elements[1:])
                    raise KeyError('Trying to replace key "{}" in env, '
                                   'but it does not exist'
                                   .format(dotted_path))

                to_edit[elements[-1]] = env._expander(new_value,
                                                      elements[1:-1])

            try:
                res = fn(Env(), *args, **kwargs)
            except Exception as e:
                Env.end()
                raise e

            Env.end()

            return res

        return wrapper

    if isinstance(source, types.FunctionType):
        fn = source
        source = None
        return decorator(fn)

    return decorator
