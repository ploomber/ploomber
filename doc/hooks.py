import os
from pathlib import Path
import subprocess
import shutil
from urllib import request
import zipfile
import jupytext
import papermill


def config_init(app, config):

    Path('projects-tmp/').mkdir()

    os.chdir('projects-tmp')

    url = 'https://github.com/ploomber/projects/archive/master.zip'
    request.urlretrieve(url, 'master.zip')

    with zipfile.ZipFile('master.zip', 'r') as f:
        f.extractall('.')

    folders = ['parametrized']

    nb = jupytext.read('projects-master/parametrized/nb.md')
    jupytext.write(nb, 'projects-master/parametrized/nb.ipynb')

    if Path('projects-master/parametrized/setup.sh').exists():
        subprocess.run('cd projects-master/parametrized && bash setup.sh',
                       shell=True,
                       check=True)

    papermill.execute_notebook('projects-master/parametrized/nb.ipynb',
                               'projects-master/parametrized/nb.ipynb',
                               kernel_name='python3',
                               cwd='projects-master/parametrized')

    shutil.copy('projects-master/parametrized/nb.ipynb',
                '../user-guide/parametrized.ipynb')

    os.chdir('..')

    shutil.rmtree('projects-tmp/')