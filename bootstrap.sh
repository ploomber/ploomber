conda create --name tmp python=3.6 --yes
conda activate tmp
pip install .
cd doc
make html