# this is used in test_env.py
mkdir tmp/
cd tmp
curl -O -L https://github.com/ploomber/template/archive/master.zip
unzip master.zip
rm -f master.zip
python template-master/install.py --name sample_project
pip install .
cd ..
rm -rf tmp/