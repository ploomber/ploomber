import types
from functools import wraps
from inspect import signature

from ploomber.env.env import Env
from ploomber.env.EnvDict import EnvDict


def _validate_and_modify_signature(fn):
    sig = signature(fn)

    if not len(sig.parameters):
        raise RuntimeError('Function "{}" does not take arguments, '
                           '@with_env decorated functions should '
                           'have env as their first artgument'
                           .format(fn.__name__))

    if list(sig.parameters.keys())[0] != 'env':
        raise RuntimeError('Function "{}" does not "env" as its first '
                           'argument, which is required to use the '
                           '@with_env decorator'
                           .format(fn.__name__))

    # https://www.python.org/dev/peps/pep-0362/#examples
    new_sig = sig.replace(parameters=tuple(sig.parameters.values())[1:])
    fn.__signature__ = new_sig

    # TODO: check no arg in the function starts with env (other than env)


def load_env(fn):
    """
    A function decorated with @load_env will be called with the current
    environment in an env keyword argument

    """
    _validate_and_modify_signature(fn)

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
    "env", the env will be passed automatically when calling the function. The
    original function's signature is edited.

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
        _validate_and_modify_signature(fn)
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
                # convert env__a__b__c -> ['a', 'b', 'c']
                keys_all = key.split('__')[1:]

                # catch errors here so we end the env if anything goes
                # wrong
                try:
                    env._replace_value(new_value, keys_all)
                except Exception as e:
                    Env.end()
                    raise KeyError('Failed to replace value using '
                                   '{}'
                                   .format(key)) from e

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
