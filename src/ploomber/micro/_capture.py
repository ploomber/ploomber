from pathlib import Path
import re
import contextlib
import inspect
import os
import sys
from functools import partial

import debuglater
import nbconvert
import nbformat
import parso

from ploomber_engine.ipython import PloomberClient
from ploomber.exceptions import TaskBuildError
from ploomber.tasks.tasks import _unserialize_params
from ploomber.util.debug import debug_if_exception

from ploomber.micro._task import _PythonCallableNoValidation
from ploomber.sources.interact import split_statement, indentation_idx


class _CapturedPythonCallable(_PythonCallableNoValidation):
    """A subclass of PythonCallable that allows capturing output
    """

    def run(self):
        if "upstream" in self.params and self._unserializer:
            # self.params["upstream"]["ones"] =
            # self.params["upstream"]["ones"][
            #     "return"]
            params = _unserialize_params(self.params, self._unserializer)
        else:
            params = self.params.to_dict()

        # do not pass product if serializer is set, we'll use the returned
        # value in such case
        if self._serializer:
            product = params.pop("product")
        else:
            product = params["product"]

        # NOTE: if the notebook contains too many definitions,
        # it is more likely that __ploomber_globals__ will contain some
        # non-pickable objects - there isn't a simple way around this. it's
        # also state dependent (e.g. .build() runs from a clean state vs
        # it runs after running cells out of order. perhaps we should add
        # on this and discourage the parallel executor on Jupyter?)
        callable_ = partial(
            _capture_execute,
            function=self.source.primitive,
            globals_=self.source.primitive.__ploomber_globals__,
            **params,
        )

        if self.debug_mode == "later":
            try:
                nb, out = callable_()
            except Exception as e:
                debuglater.run(self.name, echo=False)
                path_to_dump = f"{self.name}.dump"
                message = (f"Serializing traceback to: {path_to_dump}. "
                           f"To debug: dltr {path_to_dump}")
                raise TaskBuildError(message) from e
        elif self.debug_mode == "now":
            nb, out = debug_if_exception(callable_=callable_,
                                         task_name=self.name)
        else:
            nb, out = callable_()

        Path(self.product['nb_ipynb']).write_text(nbformat.v4.writes(nb))
        _to_html(self.product["nb_html"], nb)

        # serialize output if needed
        if self._serializer:
            if out is None:
                raise ValueError("Callable {} must return a value if task "
                                 "is initialized with a serializer".format(
                                     self.source.primitive))
            else:
                self._serializer(out, product["return"])


@contextlib.contextmanager
def _add_to_sys_path(path, chdir=False):
    """
    Add directory to sys.path, optionally making it the working directory
    temporarily
    """
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


class _Client(PloomberClient):
    """A subclass of PloomberClient that looks for a "return value"
    statement and returns the value
    """

    def _execute(self):
        execution_count = 1
        returned_value = None

        with _add_to_sys_path("."):
            for index, cell in enumerate(self._nb.cells):
                if cell.cell_type == "code":

                    try:
                        self.execute_cell(
                            cell,
                            cell_index=index,
                            execution_count=execution_count,
                            store_history=False,
                        )
                    except SyntaxError as e:
                        err = e
                    else:
                        err = None
                        tag = _parse_tag(cell.source)

                        if tag:
                            cell.metadata = dict(tags=[tag])

                    if err and str(err.msg) == "'return' outside function":
                        r = parso.parse(cell.source.strip()).children[0]
                        var = r.children[-1]
                        returned_value = self._shell.user_ns[var.value]
                        # do not show error traceback
                        cell.outputs = []

                    execution_count += 1

        return self._nb, returned_value


def _capture_execute(function, globals_, **kwargs):
    """Execute functions as notebooks
    """
    nb = nbformat.v4.new_notebook()

    for statement in _get_body_statements(function):
        c = nbformat.v4.new_code_cell(source=statement)
        nb.cells.append(c)

    upstream = kwargs.pop('upstream', {})

    # FIXME: there something weird going on. the content of globals_ changes
    # after a few tasks, it's the same object and then all of a sudden, it
    # changes (I checked with id(globals_))
    with _Client(nb) as client:

        client._shell.user_ns = {
            **client._shell.user_ns,
            **globals_,
            **upstream,
            **kwargs
        }

        nb, val = client._execute()

    return nb, val


def _get_body_statements(function):
    """Extract function statements
    """
    source = inspect.getsource(function)

    # get first non empty child, the function is usually the first one. one
    # case where it's not is when the function is indented (e.g. a function
    # inside a function)
    for child in parso.parse(source).children:
        if child.get_code().strip():
            fn = child
            break

    # NOTE: I think we can remove the while loop
    while fn.children[0].type in {'decorator', 'decorators'}:
        fn = fn.children[1]

    body_elements = [
        st for st in fn.children[-1].children if st.get_code().strip()
    ]

    return _deindent(body_elements)


def _deindent(body_elements):
    deindented = []

    for statement in body_elements:
        lines, _ = split_statement(statement)

        # find indentation # of characters using the first line
        idx = indentation_idx(lines[0])

        # remove indentation from all function body lines
        lines = [line[idx:] for line in lines]

        deindented.append('\n'.join(lines))

    return deindented


def _to_html(path, nb):
    """Convert a notebook to HTML
    """
    html = nbconvert.export(nbconvert.exporters.HTMLExporter,
                            nb,
                            exclude_input=True)[0]
    Path(path).write_text(html, encoding='utf-8')


def _parse_tag(source):
    """Given a chunk of code (str), extract the tag, if any
    """
    tag_maybe = source.strip().splitlines()[0]
    result = re.match(r'# tag=([\w-]+)', tag_maybe)

    if result:
        return result.group(1)
