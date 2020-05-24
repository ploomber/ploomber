from pathlib import Path
import tempfile
import inspect

import jupyter_client
import papermill
import parso
import nbformat


class CallableDebugger:
    """

    Examples
    --------
    >>> CallableDebugger(fn, {'param': 1})
    >>> tmp = debugger._to_nb()
    >>> tmp # modify tmp notebook
    >>> debugger._overwrite_from_nb(tmp)
    """

    def __init__(self, fn, params):
        self.fn = fn
        self.file = inspect.getsourcefile(fn)
        # how do we deal with def taking more than one line?
        lines, start = inspect.getsourcelines(fn)
        self.lines = (start, start + len(lines))
        self.params = params
        _, self.tmp_path = tempfile.mkstemp(suffix='.ipynb')
        self.body_start = None

    def _to_nb(self):
        """
        Returns the function's body in a notebook (tmp location), insert
        injected parameters
        """
        body_elements, self.body_start = parse(self.fn)
        body_to_nb(body_elements, self.tmp_path)

        # TODO: inject cells with imports + functions + classes defined
        # in the file (should make this the cwd for imports in non-packages
        # to work though) - if i do that, then I should probably do the same
        # for notebook runner to be consistent

        papermill.execute_notebook(self.tmp_path, self.tmp_path,
                                   prepare_only=True,
                                   parameters=self.params)

        return self.tmp_path

    def _overwrite_from_nb(self, path):
        """
        Overwrite the function's body with the notebook contents, excluding
        injected parameters and cells whose first line is #
        """
        # add leading space
        nb = nbformat.read(path, as_version=nbformat.NO_CONVERT)

        code_cells = [c['source'] for c in nb.cells if c['cell_type'] == 'code'
                      and 'injected-parameters'
                      not in c['metadata'].get('tags', [])
                      and c['source'][:2] != '#\n']

        # add 4 spaces to each code cell, exclude white space lines
        code_cells = [indent_cell(code) for code in code_cells]

        content = Path(self.file).read_text().splitlines()

        # first element: where the function starts + offset due to signature
        # line break
        # second: new body
        # third: the rest of the file
        keep_until = self.lines[0] + self.body_start
        new_content = (content[:keep_until]
                       + code_cells + ['\n'] + content[self.lines[1]:])

        Path(self.file).write_text('\n'.join(new_content))

    def __enter__(self):
        self.tmp_path = self._to_nb()
        return self.tmp_path

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._overwrite_from_nb(self.tmp_path)
        Path(self.tmp_path).unlink()

    def __del__(self):
        tmp = Path(self.tmp_path)
        if tmp.exists():
            tmp.unlink()


def indent_line(l):
    return '    ' + l if l else ''


def indent_cell(code):
    return '\n'.join([indent_line(l) for l in code.splitlines()])


def parse(fn):
    # TODO: exclude return at the end, what if we find more than one?
    # maybe do not support functions with return statements for now
    # getsource adds a new line at the end of the the function, we don't need
    # this
    s = inspect.getsource(fn).rstrip()
    module = parso.parse(s)
    body = module.children[0].children[-1]

    # parso is adding a new line as first element, not sure if this
    # happens always though
    if isinstance(body.children[0], parso.python.tree.Newline):
        body_elements = body.children[1:]
    else:
        body_elements = body.children

    # return body and the last line of the signature
    return body_elements, body.start_pos[0] - 1


def body_to_nb(body_elements, path):
    """
    Converts a Python function to a notebook
    """
    nb_format = nbformat.versions[nbformat.current_nbformat]
    nb = nb_format.new_notebook()

    for statement in body_elements:
        # parso incluses new line tokens, remove any trailing whitespace
        lines = [l[4:] for l in statement.get_code().rstrip().split('\n')]
        cell = nb_format.new_code_cell(source='\n'.join(lines))
        nb.cells.append(cell)

    k = jupyter_client.kernelspec.get_kernel_spec('python3')

    nb.metadata.kernelspec = {
        "display_name": k.display_name,
        "language": k.language,
        "name": 'python3'
    }

    nbformat.write(nb, path)
