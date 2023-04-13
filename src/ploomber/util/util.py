import sys
import os
import warnings
from pathlib import Path, WindowsPath
import base64
import shutil
import inspect
from collections.abc import Iterable
from contextlib import contextmanager

from ploomber.exceptions import (
    CallbackSignatureError,
    CallbackCheckAborted,
    TaskRenderError,
)
from ploomber.util.dotted_path import DottedPath


def _make_requires_error_message(missing_pkgs, fn_name, extra_msg):
    names_str = " ".join(repr(pkg) for pkg in missing_pkgs)

    error_msg = "{} {} required to use {}. Install with: " "pip install {}".format(
        names_str,
        "is" if len(missing_pkgs) == 1 else "are",
        repr(fn_name),
        names_str,
    )

    if extra_msg:
        error_msg += "\n" + extra_msg

    return error_msg


def check_mixed_envs(env_dependencies):
    # see: https://github.com/ploomber/soopervisor/issues/67
    env_dependencies = env_dependencies.split("\n")
    problematic_dependencies = [dep for dep in env_dependencies if " @ file://" in dep]
    if problematic_dependencies:
        warnings.warn(
            "Found pip dependencies installed from local files.\n"
            "This usually happens when using conda and pip"
            " in the same environment, this will break "
            "installation of new environments from the lock file."
            " Problematic "
            f"dependencies:\n{problematic_dependencies}"
        )


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


def svg2html():
    html = '<svg id="dag"></svg>'
    return html


def isiterable(obj):
    try:
        iter(obj)
    except TypeError:
        return False
    else:
        return True


def isiterable_not_str(obj):
    return isinstance(obj, Iterable) and not isinstance(obj, str)


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

    Raises
    ------
    ploomber.exceptions.CallbackCheckAborted
        When passing a dotted path whose underlying function hasn't been
        imported
    ploomber.exceptions.CallbackSignatureError
        When fn does not have the required signature
    """
    # keep a copy of the original value because we'll modified it if this is
    # a DottedPath
    available_raw = available

    if isinstance(fn, DottedPath):
        available = {**fn._spec.get_kwargs(), **available}

        if fn.callable is None:
            raise CallbackCheckAborted(
                "Cannot check callback because function "
                "is a dotted path whose function has not been imported yet"
            )
        else:
            fn = fn.callable

    parameters = inspect.signature(fn).parameters
    optional = {
        name for name, param in parameters.items() if param.default != inspect._empty
    }
    # not all functions have __name__ (e.g. partials)
    fn_name = getattr(fn, "__name__", fn)

    if optional and not allow_default:
        raise CallbackSignatureError(
            "Callback functions cannot have "
            "parameters with default values, "
            'got: {} in "{}"'.format(optional, fn_name)
        )

    required = {
        name for name, param in parameters.items() if param.default == inspect._empty
    }

    available_set = set(available)
    extra = required - available_set

    if extra:
        raise CallbackSignatureError(
            'Callback function "{}" unknown '
            "parameter(s): {}, available ones are: "
            "{}".format(fn_name, extra, available_set)
        )

    return {k: v for k, v in available_raw.items() if k in required}


def signature_check(fn, params, task_name):
    """
    Verify if the function signature used as source in a PythonCallable
    task matches available params
    """

    params = set(params)
    parameters = inspect.signature(fn).parameters

    required = {
        name for name, param in parameters.items() if param.default == inspect._empty
    }

    extra = params - set(parameters.keys())
    missing = set(required) - params

    errors = []

    if extra:
        msg = f"Got unexpected arguments: {sorted(extra)}"
        errors.append(msg)

    if missing:
        msg = f"Missing arguments: {sorted(missing)}"
        errors.append(msg)

    if "upstream" in missing:
        errors.append(
            "Verify this task declared upstream depedencies or "
            'remove the "upstream" argument from the function'
        )

    missing_except_upstream = sorted(missing - {"upstream"})

    if missing_except_upstream:
        errors.append(f'Pass {missing_except_upstream} in "params"')

    if extra or missing:
        msg = ". ".join(errors)
        # not all functions have __name__ (e.g. partials)
        fn_name = getattr(fn, "__name__", fn)
        raise TaskRenderError(
            'Error rendering task "{}" initialized with '
            'function "{}". {}'.format(task_name, fn_name, msg)
        )

    return True


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
        path = str(path).replace("\\", "\\\\")

    return f'os.chdir("{path}")'


def remove_dir(dir):
    is_dir_exists = os.path.isdir(dir)

    if is_dir_exists:
        shutil.rmtree(dir)
