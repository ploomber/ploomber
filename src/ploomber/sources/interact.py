"""
One of Ploomber's main goals is to allow writing robust/reliable code in an
interactive way. Interactive workflows make people more productive but they
might come in detriment of writing high quality code (e.g. developing a
pipeline in a single ipynb file). The basic idea for this module is to provide
a way to transparently go back and forth between a Task in a DAG and a
temporary Jupyter notebook. Currently, we only provide this for PythonCallable
and NotebookRunner but the idea is to expand to other tasks, so we have to
decide on a common behavior for this, here are a few rules:

1) Temporary jupyter notebook are usually destroyed when the user closes the
jupyter applciation. But there are extraordinary cases where we don't want to
remove it, as it might cause code loss. e.g. if the user calls
PythonCallable.develop() and while it is editing the notebook the module where
the source function is defined, we risk corrupting the module file, so we abort
overriding changes but still keep the temporary notebook. For this reason,
we save temporary notebooks in the same location of the source being edited,
to make it easier to recognize which file is related to.

2) The current working directory (cwd) in the session where Task.develop() is
called can be different from the cwd in the Jupyter application. This happens
because Jupyter sets the cwd to the current parent folder, this means that
any relative path defined in the DAG, will break if the cwd in the Jupyter app
is not the same as in the DAg declaration. To fix this, we always add a top
cell in temporary notebooks to make the cwd the same folder where
Task.develop() was called.

3) [TODO] all temporary cells must have a tmp- preffx


TODO: move the logic that implements NotebookRunner.{develop, debug} to this
module
"""
import importlib
from itertools import chain
from pathlib import Path
import inspect
import warnings

import jupyter_client
# papermill is importing a deprecated module from pyarrow
with warnings.catch_warnings():
    warnings.simplefilter('ignore', FutureWarning)
    from papermill.translators import PythonTranslator
import parso
import nbformat

from ploomber.util import chdir_code
from ploomber.sources.nb_utils import find_cell_with_tag
from ploomber.static_analysis.python import PythonCallableExtractor
from ploomber.sources.inspect import getfile

# TODO: test for locally defined objects
# TODO: reloading the fn causes trobule if it enters into an inconsistent
# state, e.g. a module that does not exist is saved, next time is realoaded,
# it will fail because it has to import such module
# TODO: if we remove upstream refernces from the functions body from jupyter
# the parameter is deleted from the signature but on reload (dag.render())
# signature validation fails bc it still loads the old signature, two options:
# either force reload all modules from all pythoncallables, or re-implement
# the signature check to get the signature using static analysis, not sure
# which is best


class CallableInteractiveDeveloper:
    """Convert callables to notebooks, edit and save back

    Parameters
    ----------
    fn : callable
        Function to edit
    params : dict
        Parameters to call the function

    Examples
    --------
    >>> wih CallableInteractiveDeveloper(fn, {'param': 1}) as path_to_nb:
    ...     # do stuff with the notebook file
    ...     pass
    """
    def __init__(self, fn, params):
        self.fn = fn
        self.path_to_source = Path(inspect.getsourcefile(fn))
        self.params = params
        self.tmp_path = self.path_to_source.with_name(
            self.path_to_source.with_suffix('').name + '-tmp.ipynb')
        self._source_code = None

    def _reload_fn(self):
        # force to reload module to get the right information in case the
        # original source code was modified and the function is no longer in
        # the same position
        # NOTE: are there any  problems with this approach?
        # we could also read the file directly and use ast/parso to get the
        # function's information we need
        mod = importlib.reload(inspect.getmodule(self.fn))
        self.fn = getattr(mod, self.fn.__name__)

    def to_nb(self, path=None):
        """
        Converts the function to is notebook representation, Returns a
        notebook object, if path is passed, it saves the notebook as well
        Returns the function's body in a notebook (tmp location), inserts
        params as variables at the top
        """
        self._reload_fn()

        body_elements, _ = parse_function(self.fn)
        top, local, bottom = extract_imports(self.fn)
        return function_to_nb(body_elements, top, local, bottom, self.params,
                              self.fn, path)

    def overwrite(self, obj):
        """
        Overwrite the function's body with the notebook contents, excluding
        injected parameters and cells whose first line is "#". obj can be
        either a notebook object or a path
        """
        self._reload_fn()

        if isinstance(obj, (str, Path)):
            nb = nbformat.read(obj, as_version=nbformat.NO_CONVERT)
        else:
            nb = obj

        nb.cells = nb.cells[:last_non_empty_cell(nb.cells)]

        # remove cells that are only needed for the nb but not for the function
        code_cells = [c['source'] for c in nb.cells if keep_cell(c)]

        # add 4 spaces to each code cell, exclude white space lines
        code_cells = [indent_cell(code) for code in code_cells]

        # get the original file where the function is defined
        content = self.path_to_source.read_text()
        content_lines = content.splitlines()
        trailing_newline = content[-1] == '\n'

        # an upstream parameter
        fn_starts, fn_ends = function_lines(self.fn)

        # keep the file the same until you reach the function definition plus
        # an offset to account for the signature (which might span >1 line)
        _, body_start = parse_function(self.fn)
        keep_until = fn_starts + body_start
        header = content_lines[:keep_until]

        # the footer is everything below the end of the original definition
        footer = content_lines[fn_ends:]

        # if there is anything at the end, we have to add an empty line to
        # properly end the function definition, if this is the last definition
        # in the file, we don't have to add this
        if footer:
            footer = [''] + footer

        new_content = '\n'.join(header + code_cells + footer)

        # replace old top imports with new ones
        new_content_lines = new_content.splitlines()
        _, line = extract_imports_top(parso.parse(new_content),
                                      new_content_lines)
        imports_top_cell, _ = find_cell_with_tag(nb, 'imports-top')

        # ignore trailing whitespace in top imports cell but keep original
        # amount of whitespace separating the last import and the first name
        # definition
        content_to_write = (imports_top_cell['source'].rstrip() + '\n' +
                            '\n'.join(new_content_lines[line - 1:]))

        # if the original file had a trailing newline, keep it
        if trailing_newline:
            content_to_write += '\n'

        # NOTE: this last part parses the code several times, we can improve
        # performance by only parsing once
        m = parso.parse(content_to_write)
        fn_def = find_function_with_name(m, self.fn.__name__)
        fn_code = fn_def.get_code()

        has_upstream_dependencies = PythonCallableExtractor(
            fn_code).extract_upstream()
        upstream_in_func_sig = upstream_in_func_signature(fn_code)

        if not upstream_in_func_sig and has_upstream_dependencies:
            fn_code_new = add_upstream_to_func_signature(fn_code)
            content_to_write = _replace_fn_source(content_to_write, fn_def,
                                                  fn_code_new)

        elif upstream_in_func_sig and not has_upstream_dependencies:
            fn_code_new = remove_upstream_to_func_signature(fn_code)
            content_to_write = _replace_fn_source(content_to_write, fn_def,
                                                  fn_code_new)

        self.path_to_source.write_text(content_to_write)

    def __enter__(self):
        self._source_code = self.path_to_source.read_text()
        self.to_nb(path=self.tmp_path)
        return str(self.tmp_path)

    def __exit__(self, exc_type, exc_val, exc_tb):
        current_source_code = self.path_to_source.read_text()

        if self._source_code != current_source_code:
            raise ValueError(f'File "{self.path_to_source}" (where '
                             f'callable "{self.fn.__name__}" is defined) '
                             'changed while editing the function in the '
                             'notebook app. This might lead to corrupted '
                             'source files. Changes from the notebook were '
                             'not saved back to the module. Notebook '
                             f'available at "{self.tmp_path}')

        self.overwrite(self.tmp_path)
        Path(self.tmp_path).unlink()

    def __del__(self):
        tmp = Path(self.tmp_path)
        if tmp.exists():
            tmp.unlink()


def last_non_empty_cell(cells):
    """Returns the index + 1 for the last non-empty cell
    """
    idx = len(cells)

    for cell in cells[::-1]:
        if cell.source:
            return idx

        idx -= 1

    return idx


def keep_cell(cell):
    """
    Rule to decide whether to keep a cell or not. This is executed before
    converting the notebook back to a function
    """
    cell_tags = set(cell['metadata'].get('tags', {}))

    # remove cell with this tag, they are not part of the function body
    tags_to_remove = {
        'injected-parameters',
        'imports-top',
        'imports-local',
        'imports-bottom',
        'debugging-settings',
    }

    has_tags_to_remove = len(cell_tags & tags_to_remove)

    return (cell['cell_type'] == 'code' and not has_tags_to_remove
            and cell['source'][:2] != '#\n')


def indent_line(lline):
    return '    ' + lline if lline else ''


def indent_cell(code):
    return '\n'.join([indent_line(line) for line in code.splitlines()])


def body_elements_from_source(source):
    # getsource adds a new line at the end of the the function, we don't need
    # this

    body = parso.parse(source).children[0].children[-1]

    # parso is adding a new line as first element, not sure if this
    # happens always though
    if isinstance(body.children[0], parso.python.tree.Newline):
        body_elements = body.children[1:]
    else:
        body_elements = body.children

    return body_elements, body.start_pos[0] - 1


def parse_function(fn):
    """
    Extract function's source code, parse it and return function body
    elements along with the # of the last line for the signature (which
    marks the beginning of the function's body) and all the imports
    """
    # TODO: exclude return at the end, what if we find more than one?
    # maybe do not support functions with return statements for now
    source = inspect.getsource(fn).rstrip()
    body_elements, start_pos = body_elements_from_source(source)
    return body_elements, start_pos


def extract_imports(fn):
    source = Path(getfile(fn)).read_text()

    module = parso.parse(source)
    lines = source.splitlines()

    imports_top, line = extract_imports_top(module, lines)

    # any imports below the top imports
    lines_bottom = '\n'.join(lines[line - 1:])
    imports_bottom = '\n'.join(
        imp.get_code() for imp in parso.parse(lines_bottom).iter_imports())

    # generate imports from local definitions
    imports_local = make_import_from_definitions(module, fn)

    return (
        imports_top,
        imports_local,
        imports_bottom if imports_bottom else None,
    )


def extract_imports_top(module, lines):
    ch = module.children[0]

    while True:
        if ch:
            if not has_import(ch):
                break
        else:
            break

        ch = ch.get_next_sibling()

    line, _ = ch.start_pos

    # line numbers start at 1...
    imports_top = '\n'.join(lines[:line - 1])
    new_lines = trailing_newlines(imports_top)

    return imports_top[:-new_lines], line - new_lines


def has_import(stmt):
    """
    Check if statement contains an import
    """
    for ch in stmt.children:
        if ch.type in {'import_name', 'import_from'}:
            return True
    return False


def trailing_newlines(s):
    n = 0

    for char in reversed(s):
        if char != '\n':
            break
        n += 1

    return n


def function_lines(fn):
    lines, start = inspect.getsourcelines(fn)
    end = start + len(lines)
    return start, end


def get_func_and_class_names(module):
    return [
        defs.name.get_code().strip()
        for defs in chain(module.iter_funcdefs(), module.iter_classdefs())
    ]


def make_import_from_definitions(module, fn):
    module_name = inspect.getmodule(fn).__name__
    names = [
        name for name in get_func_and_class_names(module)
        if name != fn.__name__
    ]

    if names:
        names_all = ', '.join(names)
        return f'from {module_name} import {names_all}'


def function_to_nb(body_elements, imports_top, imports_local, imports_bottom,
                   params, fn, path):
    """
    Save function body elements to a notebook
    """
    # TODO: Params should implement an option to call to_json_serializable
    # on product to avoid repetition I'm using this same code in notebook
    # runner. Also raise error if any of the params is not
    # json serializable
    try:
        params = params.to_json_serializable()
        params['product'] = params['product'].to_json_serializable()
    except AttributeError:
        pass

    nb_format = nbformat.versions[nbformat.current_nbformat]
    nb = nb_format.new_notebook()

    # get the module where the function is declared
    tokens = inspect.getmodule(fn).__name__.split('.')
    module_name = '.'.join(tokens[:-1])

    # add cell that chdirs for the current working directory
    # add __package__, we need this for relative imports to work
    # see: https://www.python.org/dev/peps/pep-0366/ for details
    source = """
# Debugging settings (this cell will be removed before saving)
# change the current working directory to the one when .debug() happen
# to make relative paths work
import os
{}
__package__ = "{}"
""".format(chdir_code(Path('.').resolve()), module_name)
    cell = nb_format.new_code_cell(source,
                                   metadata={'tags': ['debugging-settings']})
    nb.cells.append(cell)

    # then add params passed to the function
    cell = nb_format.new_code_cell(PythonTranslator.codify(params),
                                   metadata={'tags': ['injected-parameters']})
    nb.cells.append(cell)

    # first three cells: imports
    for code, tag in ((imports_top, 'imports-top'),
                      (imports_local, 'imports-local'), (imports_bottom,
                                                         'imports-bottom')):
        if code:
            nb.cells.append(
                nb_format.new_code_cell(source=code,
                                        metadata=dict(tags=[tag])))

    for statement in body_elements:
        lines, newlines = split_statement(statement)

        # find indentation # of characters using the first line
        idx = indentation_idx(lines[0])

        # remove indentation from all function body lines
        lines = [line[idx:] for line in lines]

        # add one empty cell per leading new line
        nb.cells.extend(
            [nb_format.new_code_cell(source='') for _ in range(newlines)])

        # add actual code as a single string
        cell = nb_format.new_code_cell(source='\n'.join(lines))
        nb.cells.append(cell)

    k = jupyter_client.kernelspec.get_kernel_spec('python3')

    nb.metadata.kernelspec = {
        "display_name": k.display_name,
        "language": k.language,
        "name": 'python3'
    }

    if path:
        nbformat.write(nb, path)

    return nb


def split_statement(statement):
    code = statement.get_code()

    newlines = 0

    for char in code:
        if char != '\n':
            break

        newlines += 1

    lines = code.strip('\n').split('\n')
    return lines, newlines


def indentation_idx(line):
    idx = len(line) - len(line.lstrip())
    return idx


def upstream_in_func_signature(source):
    _, params = _get_func_def_and_params(source)
    return 'upstream' in set(p.name.get_code().strip() for p in params
                             if p.type == 'param')


def add_upstream_to_func_signature(source):
    fn, params = _get_func_def_and_params(source)
    # add a "," if there is at least one param
    params.insert(-1, ', upstream' if len(params) > 2 else 'upstream')
    signature = try_get_code(params)
    fn.children[2] = signature
    # delete leading newline code, to avoid duplicating it
    return try_get_code(fn.children).lstrip('\n')


def remove_upstream_to_func_signature(source):
    fn, params = _get_func_def_and_params(source)
    params_names = (p.get_code().strip(', ') for p in params[1:-1])
    params_list = ', '.join(p for p in params_names if p != 'upstream')
    signature = f'({params_list})'
    fn.children[2] = signature
    # delete leading newline code, to avoid duplicating it
    return try_get_code(fn.children).lstrip('\n')


def _get_func_def_and_params(source):
    fn = parso.parse(source).children[0]

    if fn.type != 'funcdef':
        raise ValueError('Expected first element from parse source'
                         f' code to be "funcdef", got {fn.type!r}')

    return fn, fn.children[2].children


def _replace_fn_source(content_to_write, fn_def, fn_code_new):
    line_from, line_to = fn_def.start_pos[0], fn_def.end_pos[0]
    lines = content_to_write.splitlines()
    lines_new = (lines[:line_from - 1] + [fn_code_new] + lines[line_to - 1:])
    return '\n'.join(lines_new)


def try_get_code(elements):
    code = []

    for p in elements:
        try:
            s = p.get_code()
        except AttributeError:
            s = p

        code.append(s)

    return ''.join(code)


def find_function_with_name(module, fn_name):
    for fn_def in module.iter_funcdefs():
        if fn_def.name.get_code().strip() == fn_name:
            return fn_def
