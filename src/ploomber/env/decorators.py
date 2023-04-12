import inspect
import types
from functools import wraps
from collections.abc import Mapping

from ploomber.env.env import Env
from ploomber.env.envdict import EnvDict


def _get_function_name_w_module(fn):
    mod_name = inspect.getmodule(fn).__name__
    return ".".join((mod_name, fn.__name__))


def _validate_and_modify_signature(fn):
    sig = inspect.signature(fn)

    if not len(sig.parameters):
        raise RuntimeError(
            'Function "{}" does not take arguments, '
            "@with_env decorated functions should "
            "have env as their first artgument".format(fn.__name__)
        )

    if list(sig.parameters.keys())[0] != "env":
        raise RuntimeError(
            'Function "{}" does not "env" as its first '
            "argument, which is required to use the "
            "@with_env decorator".format(fn.__name__)
        )

    for arg in list(sig.parameters.keys())[1:]:
        if arg.lower().startswith("env"):
            raise RuntimeError(
                'Function "{}" has arguments '
                'starting with "env". Only the '
                'first one should start with "env"'.format(fn.__name__)
            )

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
        return fn(Env.load(), *args, **kwargs)

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
    directory will not affect initialization.

    Examples
    --------
    .. literalinclude:: ../../examples/short/with_env.py

    """

    def decorator(fn):
        _validate_and_modify_signature(fn)

        try:
            # FIXME: we should deprecate initializing from a decorator
            # with a dictionary, it isn't useful. leaving it for now
            if isinstance(source, Mapping):
                env_dict = EnvDict(source)
            else:
                # when the decorator is called without args, look for
                # 'env.yaml'
                env_dict = EnvDict.find(source or "env.yaml")
        except Exception as e:
            raise RuntimeError(
                "Failed to resolve environment using "
                '@with_env decorator in function "{}". '
                "Tried to call Env with argument: {}".format(
                    _get_function_name_w_module(fn), source
                )
            ) from e

        fn._env_dict = env_dict

        @wraps(fn)
        def wrapper(*args, **kwargs):
            to_replace = {k: v for k, v in kwargs.items() if k.startswith("env__")}

            for key in to_replace.keys():
                kwargs.pop(key)

            env_dict_new = env_dict._replace_flatten_keys(to_replace)

            try:
                Env._init_from_decorator(env_dict_new, _get_function_name_w_module(fn))
            except Exception as e:
                current = Env.load()
                raise RuntimeError(
                    "Failed to initialize environment using "
                    '@with_env decorator in function "{}". '
                    "Current environment: {}".format(
                        _get_function_name_w_module(fn), repr(current)
                    )
                ) from e

            Env._ref = _get_function_name_w_module(fn)

            try:
                res = fn(Env.load(), *args, **kwargs)
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
