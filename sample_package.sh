# this is used in test_env.py
mkdir tmp/
cd tmp
curl -O -L https://github.com/ploomber/template/archive/master.zip
unzip master.zip
python template-master/install.py --name sample_project
rm -f master.zip
pip install .
cd ..
rm -rf tmp/