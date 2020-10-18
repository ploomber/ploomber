# TODO: move to ploomebutils (projects repository)
import os
from pathlib import Path
import subprocess
import shutil
from urllib import request
import zipfile
import jupytext
import papermill
import nbformat


def add_links(nb, name):
    source = f'**Note:** To see the source code of this page, [click here](https://github.com/ploomber/projects/tree/master/{name}/README.md). [Or here](https://mybinder.org/v2/gh/ploomber/projects/master?filepath={name}%2FREADME.md) to launch an interactive demo.'
    version = nbformat.versions[nb.nbformat]
    cell = version.new_markdown_cell(source=source)
    nb.cells.insert(0, cell)


def process_tutorial(name):
    nb = jupytext.read(f'projects-master/{name}/README.md')

    add_links(nb, name)

    jupytext.write(nb, f'projects-master/{name}/README.ipynb')

    if Path(f'projects-master/{name}/setup/script.sh').exists():
        subprocess.run(f'cd projects-master/{name}/setup && bash script.sh',
                       shell=True,
                       check=True)
    if Path(f'projects-master/{name}/setup/script.py').exists():
        subprocess.run(f'cd projects-master/{name}/setup && python script.py',
                       shell=True,
                       check=True)

    papermill.execute_notebook(f'projects-master/{name}/README.ipynb',
                               f'projects-master/{name}/README.ipynb',
                               kernel_name='python3',
                               cwd=f'projects-master/{name}')

    shutil.copy(f'projects-master/{name}/README.ipynb',
                f'../user-guide/{name}.ipynb')


def config_init(app, config):
    tmp = Path('projects-tmp/')

    if tmp.exists():
        return

    tmp.mkdir()

    os.chdir('projects-tmp')

    path = '../../../projects-ploomber'

    if Path(path).exists():
        print('Local copy...')
        shutil.copytree(path, 'projects-master')
    else:
        print('Cloning from git...')
        git_clone()

    directories = ['parametrized', 'sql-templating', 'testing', 'debugging']

    for dir_ in directories:
        process_tutorial(dir_)

    os.chdir('..')

    shutil.rmtree('projects-tmp/')


def git_clone():
    url = 'https://github.com/ploomber/projects/archive/master.zip'
    request.urlretrieve(url, 'master.zip')

    with zipfile.ZipFile('master.zip', 'r') as f:
        f.extractall('.')
