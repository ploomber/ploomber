"""
Pre-process red.csv and white.csv
"""
from sqlalchemy import create_engine
import util
import pandas as pd


def download_dataset(product, upstream, path_to_dataset):
    path_to_dataset.mkdir(exist_ok=True)

    engine = create_engine(util.load_db_uri())

    df_training = pd.read_sql('SELECT * FROM training', engine)
    df_testing = pd.read_sql('SELECT * FROM testing', engine)

    engine.dispose()

    df_training.to_csv(path_to_dataset / 'training.csv', index=False)
    df_testing.to_csv(path_to_dataset / 'testing.csv', index=False)
