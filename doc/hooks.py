from ploomberutils.nb import process_readme_md
import os
from pathlib import Path
import shutil
from urllib import request
import zipfile

# def add_links(nb, name):
#     source = f'**Note:** To see the source code of this page, [click here](https://github.com/ploomber/projects/tree/master/{name}/README.md). [Or here](https://mybinder.org/v2/gh/ploomber/projects/master?filepath={name}%2FREADME.md) to launch an interactive demo.'
#     version = nbformat.versions[nb.nbformat]
#     cell = version.new_markdown_cell(source=source)
#     nb.cells.insert(0, cell)


def config_init(app, config):
    tmp = Path('../projects-tmp/')

    if tmp.exists():
        return

    tmp.mkdir()

    os.chdir('../projects-tmp')

    path = '../../projects-ploomber'

    if Path(path).exists():
        print('Local copy...')
        # create a symlink instead...
        shutil.copytree(path, 'projects-master')
    else:
        print('Cloning from git...')
        git_clone()

    directories = {
        'parametrized': 'user-guide',
        'sql-templating': 'user-guide',
        'testing': 'user-guide',
        'debugging': 'user-guide',
        'spec-api-python': 'get-started'
    }

    os.chdir('projects-master')

    process_readme_md(list(directories))

    for name, target_dir in directories.items():
        src = f'{name}/README.ipynb'
        dst = f'../../doc/{target_dir}/{name}.ipynb'
        print(f'Copying {src!r} to {dst!r}')
        shutil.copy(src, dst)


def git_clone():
    url = 'https://github.com/ploomber/projects/archive/master.zip'
    request.urlretrieve(url, 'master.zip')

    with zipfile.ZipFile('master.zip', 'r') as f:
        f.extractall('.')


if __name__ == '__main__':
    config_init(app=None, config=None)
