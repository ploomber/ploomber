"""
Sample red.csv and white.csv
"""

from pathlib import Path

import pandas as pd
from ploomber import Env


def sample(product, upstream):
    env = Env()

    red = pd.read_csv(str(upstream['get_data'][0]), sep=';')
    white = pd.read_csv(str(upstream['get_data'][1]), sep=';')

    red_sample = red.sample(frac=0.2)
    white_sample = white.sample(frac=0.2)

    output_path = Path(env.path.input / 'sample')
    output_path.mkdir(exist_ok=True)

    red_sample.to_csv(str(product[0]), index=False)
    white_sample.to_csv(str(product[1]), index=False)
