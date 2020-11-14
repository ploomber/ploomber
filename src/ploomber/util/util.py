import sys
import os
from pathlib import Path, WindowsPath
import importlib
from functools import wraps, reduce
import base64
import shutil
import inspect
from collections.abc import Iterable
from contextlib import contextmanager

from ploomber.exceptions import CallbackSignatureError, TaskRenderError


def requires(pkgs, name=None, extra_msg=None):
    """
    Check if packages were imported, raise ImportError with an appropriate
    message for missing ones

    Error message:
    a, b are required to use function. Install them by running pip install a b

    Parameters
    ----------
    pkgs
        The names of the packages required

    name
        The name of the module/function/class to show in the error message,
        if None, the decorated function __name__ attribute is used

    extra_msg
        Append this extra message to the end
    """
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            missing = [
                pkg for pkg in pkgs if importlib.util.find_spec(pkg) is None
            ]

            if missing:
                msg = reduce(lambda x, y: x + ' ' + y, missing)
                fn_name = name or f.__name__
                error_msg = ('{} {} required to use {}. Install {} by '
                             'running "pip install {}"'.format(
                                 msg, 'is' if len(missing) == 1 else 'are',
                                 fn_name,
                                 'it' if len(missing) == 1 else 'them', msg))

                if extra_msg:
                    error_msg += ('. ' + extra_msg)

                raise ImportError(error_msg)

            return f(*args, **kwargs)

        return wrapper

    return decorator


def safe_remove(path):
    if path.exists():
        if path.is_file():
            path.unlink()
        else:
            shutil.rmtree(path)


def image_bytes2html(data):
    fig_base64 = base64.encodebytes(data)
    img = fig_base64.decode("utf-8")
    html = '<img src="data:image/png;base64,' + img + '"></img>'
    return html


def isiterable(obj):
    try:
        iter(obj)
    except TypeError:
        return False
    else:
        return True


# TODO: add more context to errors, which task and which hook?
def callback_check(fn, available, allow_default=True):
    """
    Check if a callback function signature requests available parameters

    Parameters
    ----------
    fn : callable
        Callable (e.g. a function) to check

    available : dict
        All available params

    allow_default : bool, optional
        Whether allow arguments with default values in "fn" or not

    Returns
    -------
    dict
        Dictionary with requested parameters
    """
    parameters = inspect.signature(fn).parameters
    optional = {
        name
        for name, param in parameters.items()
        if param.default != inspect._empty
    }
    # not all functions have __name__ (e.g. partials)
    fn_name = getattr(fn, '__name__', fn)

    if optional and not allow_default:
        raise CallbackSignatureError('Callback functions cannot have '
                                     'parameters with default values, '
                                     'got: {} in "{}"'.format(
                                         optional, fn_name))

    required = {
        name
        for name, param in parameters.items()
        if param.default == inspect._empty
    }

    available_set = set(available)
    extra = required - available_set

    if extra:
        raise CallbackSignatureError('Callback function "{}" unknown '
                                     'parameter(s): {}, available ones are: '
                                     '{}'.format(fn_name, extra,
                                                 available_set))

    return {k: v for k, v in available.items() if k in required}


def signature_check(fn, params, task_name):
    """
    Verify if the function signature used as source in a PythonCallable
    task matches available params
    """
    params = set(params)
    parameters = inspect.signature(fn).parameters
    required = {
        name
        for name, param in parameters.items()
        if param.default == inspect._empty
    }

    extra = params - set(parameters.keys())
    missing = set(required) - params

    errors = []

    if extra:
        msg = ('The following params are not part of the function '
               'signature: {}'.format(extra))
        errors.append(msg)

    if missing:
        msg = 'The following params are missing: {}'.format(missing)
        errors.append(msg)

    if extra or missing:
        msg = '. '.join(errors)
        # not all functions have __name__ (e.g. partials)
        fn_name = getattr(fn, '__name__', fn)
        raise TaskRenderError('Error rendering task "{}" initialized with '
                              'function "{}". {}'.format(
                                  task_name, fn_name, msg))

    return True


def _parse_module(dotted_path, raise_=True):
    parts = dotted_path.split('.')

    if len(parts) < 2 or not all(parts):
        if raise_:
            raise ValueError('Invalid module name, must be a dot separated '
                             'string, with at least '
                             '[module_name].[function_name]')
        else:
            return False

    return '.'.join(parts[:-1]), parts[-1]


def load_dotted_path(dotted_path, raise_=True):
    """Load an object/function/module by passing a dotted path

    Parameters
    ----------
    dotted_path : str
        Dotted path to a module, e.g. ploomber.tasks.NotebookRunner
    raise_ : bool, optional
        If True, an exception is raised if the module can't be imported,
        otherwise return None if that happens
    """
    obj, module = None, None

    parsed = _parse_module(dotted_path, raise_=raise_)

    if parsed:
        mod, name = parsed

        try:
            module = importlib.import_module(mod)
        except ImportError as e:
            if raise_:
                # we want to raise ethe same error type but chaining exceptions
                # produces a long verbose output, so we just modify the
                # original message to add more context, it's ok to hide the
                # original traceback since it will just point to lines
                # in the importlib module, which isn't useful for the user
                e.msg = ('An error happened when trying to '
                         'import dotted path "{}": {}'.format(
                             dotted_path, str(e)))
                raise

        if module:
            try:
                obj = getattr(module, name)
            except AttributeError as e:
                if raise_:
                    # same as in the comment above
                    e.args = (
                        'Could not get "{}" from module '
                        '"{}" (loaded from: {}), make sure it is a valid '
                        'callable defined in such module'.format(
                            name, mod, module.__file__), )
                    raise
        return obj
    else:
        if raise_:
            raise ValueError(
                'Invalid dotted path value "{}", must be a dot separated '
                'string, with at least '
                '[module_name].[function_name]'.format(dotted_path))


def locate_dotted_path(dotted_path):
    tokens = dotted_path.split('.')
    module = '.'.join(tokens[:-1])
    spec = importlib.util.find_spec(module)

    if spec is None:
        raise ModuleNotFoundError(f'Module {module!r} does not exist')

    return spec


def find_file_recursively(name, max_levels_up=6, starting_dir=None):
    """
    Find environment by looking into the current folder and parent folders,
    returns None if no file was found otherwise pathlib.Path to the file
    """
    current_dir = starting_dir or os.getcwd()
    current_dir = Path(current_dir)
    path_to_file = None

    for _ in range(max_levels_up):
        current_path = Path(current_dir, name)

        if current_path.exists():
            path_to_file = current_path.resolve()
            break

        current_dir = current_dir.parent

    return path_to_file


def call_with_dictionary(fn, kwargs):
    """
    Call a function by passing elements from a dictionary that appear in the
    function signature
    """
    parameters = inspect.signature(fn).parameters
    common = set(parameters) & set(kwargs)
    sub_kwargs = {k: kwargs[k] for k in common}
    return fn(**sub_kwargs)


def _make_iterable(o):
    if isinstance(o, Iterable) and not isinstance(o, str):
        return o
    elif o is None:
        return []
    else:
        return [o]


@contextmanager
def add_to_sys_path(path, chdir):
    cwd_old = os.getcwd()

    if path is not None:
        path = os.path.abspath(path)
        sys.path.insert(0, path)

        if chdir:
            os.chdir(path)

    try:
        yield
    finally:
        if path is not None:
            sys.path.remove(path)
            os.chdir(cwd_old)


def chdir_code(path):
    """
    Returns a string with valid code to chdir to the passed path
    """
    path = Path(path).resolve()

    if isinstance(path, WindowsPath):
        path = str(path).replace('\\', '\\\\')

    return f'os.chdir("{path}")'
