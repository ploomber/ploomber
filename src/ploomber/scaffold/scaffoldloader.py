import ast
from pathlib import Path

import jupytext
from jinja2 import Environment, PackageLoader, StrictUndefined

try:
    import importlib.resources as resources
except ImportError:
    import importlib_resources as resources

from ploomber import tasks
from ploomber.util.dotted_path import locate_dotted_path


class ScaffoldLoader:
    def __init__(self, directory, project_name=None):
        self.env = Environment(loader=PackageLoader(
            'ploomber', str(Path('resources', directory))),
                               variable_start_string='[[',
                               variable_end_string=']]',
                               block_start_string='[%',
                               block_end_string='%]',
                               undefined=StrictUndefined)
        self.directory = directory
        self.project_name = project_name

    def get_template(self, name):
        return self.env.get_template(name)

    def render(self, name, params):
        if name == 'task.ipynb':
            p = Path(name)
            convert_to = p.suffix[1:]
            name = str(p.with_suffix('.py'))
        else:
            convert_to = None

        t = self.get_template(name)
        out = t.render(**params)

        if convert_to:
            nb = jupytext.reads(out, fmt='py:light')
            out = jupytext.writes(nb, fmt=convert_to)

        return out

    def copy(self, name):
        module = '.'.join(['ploomber', 'resources', self.directory])
        content = resources.read_text(module, name)
        Path(self.project_name, name).write_text(content)

    def create(self, source, params, class_):
        if class_ is tasks.PythonCallable:
            fn_name = source.split('.')[-1]
            params['function_name'] = fn_name
            source = Path(locate_dotted_path(source).origin)
            source.parent.mkdir(parents=True, exist_ok=True)
            original = source.read_text()

            module = ast.parse(original)

            names = {
                element.name
                for element in module.body if hasattr(element, 'name')
            }

            if fn_name not in names:
                print(f'Adding {fn_name!r} to module {source!s}...')
                fn_str = self.render('function.py', params=params)
                source.write_text(original + fn_str)
        else:
            path = Path(source)

            if not path.exists():
                if path.suffix in {'.py', '.sql', '.ipynb'}:
                    # create parent folders if needed
                    source.parent.mkdir(parents=True, exist_ok=True)
                    content = self.render('task' + source.suffix,
                                          params=params)
                    print('Adding {}...'.format(source))
                    source.write_text(content)
                else:
                    print('Error: This command does not support adding '
                          'tasks with extension "{}", valid ones are '
                          '.py and .sql. Skipping {}'.format(
                              path.suffix, path))
