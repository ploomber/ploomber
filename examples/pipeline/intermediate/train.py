"""
Pre-process red.csv and white.csv
"""
from pathlib import Path
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import pandas as pd


def train_and_save_report(product, upstream, path_to_dataset, path_to_report):
    df_training = pd.read_csv(path_to_dataset / 'training.csv')
    df_testing = pd.read_csv(path_to_dataset / 'testing.csv')

    X = df_training.drop('label', axis='columns').values
    y = df_training.label

    X_test = df_testing.drop('label', axis='columns').values
    y_test = df_testing.label

    model = RandomForestClassifier(n_estimators=10, n_jobs=-1)
    model.fit(X, y)

    y_test_pred = model.predict(X_test)

    report = classification_report(y_test, y_test_pred)

    path_to_report = Path(path_to_report)

    path_to_report.parent.mkdir(exist_ok=True)

    path_to_report.write_text(report)
