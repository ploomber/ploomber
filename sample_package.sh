# this is used in test_env.py
wget https://github.com/edublancas/ds-template/archive/master.zip
unzip master.zip
cd ds-template-master/basic-template/
mv 'src/{{package_name}}' 'src/sample_project'
sed 's/{{package_name}}/sample_project/g' setup.py > setup-tmp.py
rm -f setup.py
mv setup-tmp.py setup.py
pip install .
cd ../..
rm -rf ds-template-master