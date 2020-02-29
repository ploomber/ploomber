# this is used in test_env.py
curl -O -L https://github.com/ploomber/template/archive/master.zip
unzip master.zip
bash template-master/install.sh sample_project
rm -f master.zip
pip install sample_project/
rm -rf sample_project/