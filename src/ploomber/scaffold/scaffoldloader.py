import ast
from pathlib import Path

import jupytext
from jinja2 import Environment, PackageLoader, StrictUndefined

from ploomber import tasks
from ploomber.util.dotted_path import (locate_dotted_path,
                                       create_intermediate_modules)


class ScaffoldLoader:
    """Scaffold task files
    """

    def __init__(self):
        self.env = Environment(loader=PackageLoader(
            'ploomber', str(Path('resources', 'ploomber_add'))),
                               variable_start_string='[[',
                               variable_end_string=']]',
                               block_start_string='[%',
                               block_end_string='%]',
                               undefined=StrictUndefined)

    def get_template(self, name):
        return self.env.get_template(name)

    def render(self, name, params):
        # if requested the ipynb template, we must convert it from the .py file
        if name == 'task.ipynb':
            p = Path(name)
            convert_to = p.suffix[1:]
            name = str(p.with_suffix('.py'))
            is_ipynb = True
        else:
            convert_to = None
            is_ipynb = False

        t = self.get_template(name)
        out = t.render(**params, is_ipynb=is_ipynb)

        if convert_to:
            nb = jupytext.reads(out, fmt='py:light')
            out = jupytext.writes(nb, fmt=convert_to)

        return out

    def create(self, source, params, class_):
        """Scaffold a task if they don't exist

        Returns
        -------
        bool
            True if it created a task, False if it didn't
        """
        did_create = False

        if class_ is tasks.PythonCallable:
            source_parts = source.split('.')
            (*module_parts, fn_name) = source_parts
            params['function_name'] = fn_name

            try:
                spec = locate_dotted_path(source)
            except ModuleNotFoundError:
                create_intermediate_modules(module_parts)
                spec = locate_dotted_path(source)

            source = Path(spec.origin)
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

                did_create = True

        #  script task...
        else:
            path = Path(source)

            if not path.exists():
                if path.suffix in {'.py', '.sql', '.ipynb', '.R', '.Rmd'}:
                    # create parent folders if needed
                    source.parent.mkdir(parents=True, exist_ok=True)

                    content = self.render('task' + source.suffix,
                                          params=params)

                    print('Adding {}...'.format(source))
                    source.write_text(content)

                    did_create = True
                else:
                    print('Error: This command does not support adding '
                          'tasks with extension "{}", valid ones are '
                          '.py and .sql. Skipping {}'.format(
                              path.suffix, path))

        return did_create
