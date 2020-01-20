# pipeline example

This example shows the usage of the `dstools.pipeline` module.


```bash
DB=$(dstools env db)
INPUT=$(dstools env path.input)
HOME=$(dstools env path.home)

# get raw data
bash get_data.sh

# sample from raw data and generate sample/red.csv and sample/white.csv
python sample.py

# upload sample/red.csv and sample/white.csv
csvsql --db $DB --tables red --insert "$INPUT/sample/red.csv"  --overwrite
csvsql --db $DB --tables white --insert "$INPUT/sample/white.csv"  --overwrite

# create table with both red and white
psql $DB -f $HOME/sql/create_wine.sql

# select features and add label
psql $DB -f $HOME/sql/create_dataset.sql

# create trainng set
psql $DB -f $HOME/sql/create_training.sql

# create testing set
psql $DB -f $HOME/sql/create_testing.sql
```