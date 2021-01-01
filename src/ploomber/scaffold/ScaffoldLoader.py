from pathlib import Path

import jupytext
from jinja2 import Environment, PackageLoader

try:
    import importlib.resources as resources
except ImportError:
    import importlib_resources as resources


class ScaffoldLoader:
    def __init__(self, directory, project_name=None):
        self.env = Environment(loader=PackageLoader(
            'ploomber', str(Path('resources', directory))),
                               variable_start_string='[[',
                               variable_end_string=']]',
                               block_start_string='[%',
                               block_end_string='%]')
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
