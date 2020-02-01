"""
Traind and predict pipeline
===========================

Example showing how to build a training and a prediction pipeline
"""
import tempfile
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
    """Join raw data with generated features
    """
    a = pd.read_parquet(str(upstream['get']))
    b = pd.read_parquet(str(upstream['features']))
    df = a.join(b)
    df.to_parquet(str(product))


def _fit(upstream, product):
    """Fit model and generate classification report
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


def _pred(product, upstream, model):
    """De-serialize model and make a new prediction
    """
    clf = joblib.load(model)
    df = pd.read_parquet(str(upstream['join']))
    df = df.drop('target', axis='columns')
    score = pd.DataFrame(clf.predict_proba(df))
    score.columns = ['proba_class_0', 'proba_class_1', 'proba_class_2']
    score.to_csv(str(product), index=False)


def add_fts(dag):
    """Modify a pipeline with a "get" task to generate features and join them
    """
    fts = PythonCallable(_features, File('features.parquet'), dag,
                         name='features')
    join = PythonCallable(_join_features, File('join.parquet'), dag,
                          name='join')

    dag['get'] >> fts

    (dag['get'] + fts) >> join

    return dag


tmp_dir = Path(tempfile.mkdtemp())


# build training pipeline
dag_fit = DAG()
get = PythonCallable(_get,
                     File(tmp_dir / 'data.parquet'),
                     dag_fit,
                     name='get')
dag_fit = add_fts(dag_fit)
fit = PythonCallable(_fit, {'report': File(tmp_dir / 'report.txt'),
                            'model': File(tmp_dir / 'model.joblib')},
                     dag_fit,
                     name='fit')
dag_fit['join'] >> fit


###############################################################################
# Fit pipeline plot
dag_fit.plot(output='matplotlib')


dag_fit.build()

# build prediction pipeline - pass a new observation with values [1, 0, 10, 2]
dag_pred = DAG()
get = PythonCallable(_new_obs,
                     File(tmp_dir / 'obs.parquet'),
                     dag_pred,
                     name='get',
                     params={'values': [1, 0, 10, 2]})

dag_pred = add_fts(dag_pred)
pred = PythonCallable(_pred, File(tmp_dir / 'pred.csv'), dag_pred,
                      name='pred',
                      params={'model': tmp_dir / 'model.joblib'})

dag_pred['join'] >> pred

###############################################################################
# Prediction pipeline plot
dag_pred.plot(output='matplotlib')

dag_pred.build()

# get prediction
pd.read_csv(str(dag_pred['pred']))
