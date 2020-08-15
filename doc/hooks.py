import os
from pathlib import Path
import subprocess
import shutil
from urllib import request
import zipfile
import jupytext
import papermill


def process_tutorial(name):
    nb = jupytext.read(f'projects-master/{name}/nb.md')
    jupytext.write(nb, f'projects-master/{name}/nb.ipynb')

    if Path(f'projects-master/{name}/setup/script.sh').exists():
        subprocess.run(f'cd projects-master/{name}/setup && bash script.sh',
                       shell=True,
                       check=True)
    if Path(f'projects-master/{name}/setup/script.py').exists():
        subprocess.run(f'cd projects-master/{name}/setup && python script.py',
                       shell=True,
                       check=True)

    papermill.execute_notebook(f'projects-master/{name}/nb.ipynb',
                               f'projects-master/{name}/nb.ipynb',
                               kernel_name='python3',
                               cwd=f'projects-master/{name}')

    shutil.copy(f'projects-master/{name}/nb.ipynb',
                f'../user-guide/{name}.ipynb')


def config_init(app, config):
    tmp = Path('projects-tmp/')

    if tmp.exists():
        return

    tmp.mkdir()

    os.chdir('projects-tmp')

    url = 'https://github.com/ploomber/projects/archive/master.zip'
    request.urlretrieve(url, 'master.zip')

    with zipfile.ZipFile('master.zip', 'r') as f:
        f.extractall('.')

    directories = ['parametrized', 'sql-templating', 'testing', 'debugging']

    for dir_ in directories:
        process_tutorial(dir_)

    os.chdir('..')

    shutil.rmtree('projects-tmp/')
