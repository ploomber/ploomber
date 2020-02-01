conda remove --name tmp --all --yes
conda create --name tmp python=3.6 --yes
conda activate tmp
pip install .
pip install -r doc/requirements.txt
cd doc
make html