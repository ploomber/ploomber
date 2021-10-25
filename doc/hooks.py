from pathlib import Path
import shutil
from urllib import request
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
        projects = Path('../../projects-master')

    # key: directory in the projects directory
    # value: directory in the documentation
    directories = {
        'guides/spec-api-python': 'get-started',
        'guides/parametrized': 'user-guide',
        'guides/sql-templating': 'user-guide',
        'guides/testing': 'user-guide',
        'guides/debugging': 'user-guide',
        'guides/serialization': 'user-guide',
        'guides/logging': 'user-guide',
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


if __name__ == '__main__':
    config_init(app=None, config=None)
