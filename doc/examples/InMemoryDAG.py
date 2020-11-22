import pickle
import pandas as pd
from sklearn import datasets
from sklearn.tree import DecisionTreeClassifier
from ploomber import DAG, InMemoryDAG
from ploomber.tasks import PythonCallable
from ploomber.products import File
from ploomber.executors import Serial
from ploomber.tasks.param_forward import input_data_passer, in_memory_processor


def get(product):
    d = datasets.load_iris(as_frame=True)
    df = d['data']
    df['target'] = d['target']
    return df


def a_feature(upstream, product):
    df = upstream['get']
    return pd.DataFrame({'a_feature': df['sepal length (cm)']**2})


def another(upstream, product):
    df = upstream['get']
    return pd.DataFrame({'another': df['sepal width (cm)']**2})


def join(product, upstream):
    return upstream['get'].join(upstream['a_feature']).join(
        upstream['another'])


def fit(product, upstream):
    clf = DecisionTreeClassifier()
    df = pd.read_csv(str(upstream['join']))
    X = df.drop('target', axis='columns')
    y = df['target']
    clf.fit(X, y)

    with open(str(product), 'wb') as f:
        pickle.dump(clf, f)


def serializer(df, product):
    df.to_csv(str(product), index=False)


def unserializer(product):
    return pd.read_csv(str(product))


def add_features(dag):
    get_t = dag['get']

    a_feature_t = PythonCallable(a_feature,
                                 File('a_feature.csv'),
                                 dag,
                                 serializer=serializer,
                                 unserializer=unserializer)
    another_t = PythonCallable(another,
                               File('another.csv'),
                               dag,
                               serializer=serializer,
                               unserializer=unserializer)
    join_t = PythonCallable(join,
                            File('join.csv'),
                            dag,
                            serializer=serializer,
                            unserializer=unserializer)
    get_t >> a_feature_t
    get_t >> another_t
    (get_t + a_feature_t + another_t) >> join_t
    return dag


def make_training():
    dag = DAG(executor=Serial(build_in_subprocess=False))
    PythonCallable(get,
                   File('get.csv'),
                   dag,
                   serializer=serializer,
                   unserializer=unserializer)
    add_features(dag)
    fit_t = PythonCallable(fit, File('model.pickle'), dag)
    dag['join'] >> fit_t
    return dag


def make_predict():
    dag_pred = DAG()
    input_data_passer(dag=dag_pred, name='get')

    # we re-use the same graph that we used for training!
    add_features(dag_pred)

    def _predict(product, model, upstream):
        return model.predict(upstream['join'])

    with open('model.pickle', 'rb') as f:
        clf = pickle.load(f)

    predict = in_memory_processor(dag=dag_pred,
                                  name='predict',
                                  processor=_predict,
                                  model=clf)
    dag_pred['join'] >> predict

    # NOTE: do not call build on dag_pred directly!
    in_memory = InMemoryDAG(dag_pred)

    return in_memory


dag = make_training()
dag.build()

sample_input = pd.DataFrame({
    'sepal length (cm)': [5.9],
    'sepal width (cm)': [3],
    'petal length (cm)': [5.1],
    'petal width (cm)': [1.8]
})

dag_pred = make_predict()
result = dag_pred.build({'get': sample_input})
result['predict']
