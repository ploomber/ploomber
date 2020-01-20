"""
Using the same pipeline for fitting and predictions
"""
from pathlib import Path

from ploomber import DAG
from ploomber.tasks import PythonCallable
from ploomber.products import File

import joblib
import pandas as pd
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.ensemble import RandomForestClassifier


def _get(product):
    """Get data
    """
    d = datasets.load_iris()

    df = pd.DataFrame(d['data'])
    df.columns = d['feature_names']
    df['target'] = d['target']

    df.to_parquet(str(product))


def _features(upstream, product):
    """Generate new features from existing columns
    """
    data = pd.read_parquet(str(upstream['get']))
    ft = data['sepal length (cm)'] * data['sepal width (cm)']
    df = pd.DataFrame({'sepal area (cm2)': ft})
    df.to_parquet(str(product))


def _join_features(upstream, product):
    a = pd.read_parquet(str(upstream['get']))
    b = pd.read_parquet(str(upstream['features']))
    df = a.join(b)
    df.to_parquet(str(product))


def _fit(upstream, product):
    """Fit preprocessor and model
    """
    df = pd.read_parquet(str(upstream['join']))
    X = df.drop('target', axis='columns')
    y = df.target

    X_train, X_test, y_train, y_test = train_test_split(X, y,
                                                        test_size=0.33,
                                                        random_state=42)

    clf = RandomForestClassifier()
    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)

    report = classification_report(y_test, y_pred)

    Path(str(product['report'])).write_text(report)
    joblib.dump(clf, str(product['model']))


def _new_obs(product, values):
    """Save an observation as a data frame
    """
    df = pd.DataFrame([values + [None]])
    df.columns = ['sepal length (cm)', 'sepal width (cm)',
                  'petal length (cm)', 'petal width (cm)', 'target']
    df.to_parquet(str(product))


def _score(product, upstream, model):
    clf = joblib.load(model)
    df = pd.read_parquet(str(upstream['join']))
    df = df.drop('target', axis='columns')
    score = pd.DataFrame(clf.predict_proba(df))
    score.columns = ['proba_class_0', 'proba_class_1', 'proba_class_2']
    score.to_csv(str(product), index=False)


def make_pipeline(dag, get):
    fts = PythonCallable(_features, File('features.parquet'), dag,
                         name='features')
    join = PythonCallable(_join_features, File('join.parquet'), dag,
                          name='join')

    get >> fts

    (get + fts) >> join

    return dag


dag_fit = DAG()
get = PythonCallable(_get, File('data.parquet'), dag_fit, name='get')
make_pipeline(dag_fit, get)
fit = PythonCallable(_fit, {'report': File('report.txt'),
                            'model': File('model.joblib')}, dag_fit,
                     name='fit')

dag_fit['join'] >> fit


dag_pred = DAG()
get = PythonCallable(_new_obs, File('obs.parquet'), dag_pred, name='get',
                     params={'values': [1, 0, 10, 2]})
make_pipeline(dag_pred, get)
score = PythonCallable(_score, File('pred.txt'), dag_pred,
                       name='pred',
                       params={'model': 'model.joblib'})

dag_pred['join'] >> score
