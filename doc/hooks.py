import os
from pathlib import Path
import shutil
from urllib import request
from urllib.parse import quote_plus
import zipfile


def config_init(app, config):

    # copy outside the doc folder, otherwise sphinx thinks all those files
    # should be part of the documentation
    projects = Path('../../projects-ploomber/')

    if Path(projects).exists():
        print('Using local copy...')

    else:
        print('Cloning from git...')
        git_clone()

    # key: directory in the projects directory
    # value: directory in the documentation
    directories = {
        'guides/first-pipeline': 'get-started',
        'guides/parametrized': 'user-guide',
        'guides/sql-templating': 'user-guide',
        'guides/testing': 'user-guide',
        'guides/debugging': 'user-guide',
        'guides/serialization': 'user-guide',
        'guides/logging': 'user-guide',
        'guides/versioning': 'user-guide',
        'guides/cloud-execution': 'cloud',
        'cookbook/report-generation': 'cookbook',
    }

    # move README.ipynb files to their corresponding location in the docs
    for path, target_dir in directories.items():
        src = Path(projects, path, 'README.ipynb')
        name = Path(path).name
        dst = Path(target_dir, f'{name}.ipynb')
        print(f'Copying {src} to {dst}')
        shutil.copy(src, dst)


def git_clone():
    url = 'https://github.com/ploomber/projects/archive/master.zip'
    request.urlretrieve(url, '../../master.zip')

    with zipfile.ZipFile('../../master.zip', 'r') as f:
        f.extractall('../../')

    os.rename('../../projects-master', Path('../../projects-ploomber'))


def jinja_filters(app):
    try:
        app.builder.templates.environment.filters['quote_plus'] = quote_plus
    except AttributeError:
        # .templates only valid for the HTML builder, rtd also converts to
        # other formats
        pass


if __name__ == '__main__':
    config_init(app=None, config=None)
