"""
This example shows how to re-use the same feature engineering code in
both training (batch processing) and serving (online).
"""

import pickle
from pathlib import Path

import pandas as pd
from sklearn import datasets
from sklearn.tree import DecisionTreeClassifier

from ploomber import DAG, InMemoryDAG
from ploomber.tasks import PythonCallable
from ploomber.products import File
from ploomber.executors import Serial
from ploomber.tasks import input_data_passer, in_memory_callable


def get():
    """Get training data"""
    d = datasets.load_iris(as_frame=True)
    df = d["data"]
    df["target"] = d["target"]
    return df


# NOTE: "upstream" is the output from the task that executes before this one
def a_feature(upstream):
    """Compute one feature"""
    df = upstream["get"]
    return pd.DataFrame({"a_feature": df["sepal length (cm)"] ** 2})


def another(upstream):
    """Compute another feature"""
    df = upstream["get"]
    return pd.DataFrame({"another": df["sepal width (cm)"] ** 2})


def join(upstream):
    return upstream["get"].join(upstream["a_feature"]).join(upstream["another"])


# NOTE: "product" is the model file output location
def fit(product, upstream):
    """Train a model and save it (pickle format)"""
    clf = DecisionTreeClassifier()
    df = pd.read_csv(str(upstream["join"]))
    X = df.drop("target", axis="columns")
    y = df["target"]
    clf.fit(X, y)

    with open(str(product), "wb") as f:
        pickle.dump(clf, f)


# NOTE: serializer and unserializer are special function that tell the pipeline
# how to convert the object returned by our tasks (pandas.DataFrame) to files.
# These are only required when we want to build a dag that works both in
# batch-processing and online mode
def serializer(df, product):
    """Save all data frames as CSVs"""
    out = str(product)

    # make sure the parent folder exists
    Path(out).parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(out, index=False)


def unserializer(product):
    """Function to read CSVs"""
    return pd.read_csv(str(product))


def add_features(dag):
    """
    Given a DAG, adds feature engineering tasks. The DAG must have a task "get"
    that returns the input data.
    """
    get_task = dag["get"]

    output = Path("output")

    # instantiate tasks
    a_feature_task = PythonCallable(
        a_feature,
        File(output / "a_feature.csv"),
        dag,
        serializer=serializer,
        unserializer=unserializer,
    )
    another_task = PythonCallable(
        another,
        File(output / "another.csv"),
        dag,
        serializer=serializer,
        unserializer=unserializer,
    )
    join_task = PythonCallable(
        join,
        File(output / "join.csv"),
        dag,
        serializer=serializer,
        unserializer=unserializer,
    )

    # establish dependencies
    get_task >> a_feature_task
    get_task >> another_task
    (get_task + a_feature_task + another_task) >> join_task

    return dag


def make_training():
    """Instantiates the training DAG"""
    # setting build_in_subprocess=False because Python does not like when we
    # use multiprocessing in functions defined in the main module. Works if
    # we define them in a different one
    dag = DAG(executor=Serial(build_in_subprocess=False))

    output = Path("output")

    # add "get" task that returns the training data
    PythonCallable(
        get,
        File(output / "get.csv"),
        dag,
        serializer=serializer,
        unserializer=unserializer,
    )

    # add features tasks
    add_features(dag)

    # add "fit" task for model training
    fit_t = PythonCallable(fit, File(output / "model.pickle"), dag)

    # train after joining features
    dag["join"] >> fit_t

    return dag


def predict(upstream, model):
    """Make a prediction after computing features"""
    return model.predict(upstream["join"])


def validate_input_data(df):
    """
    Validate input data
    """
    cols = [
        "sepal length (cm)",
        "sepal width (cm)",
        "petal length (cm)",
        "petal width (cm)",
    ]

    if list(df.columns) != cols:
        raise ValueError(f"Unexpected set of columns, expected: {cols!r}")

    is_negative = df.min() < 0
    wrong_cols = list(is_negative[is_negative].index)

    if len(wrong_cols):
        raise ValueError(
            f"Column(s) {wrong_cols!r} have one or more invalid"
            " (negative) observations"
        )

    return df


def make_predict():
    """Instantiate a prediction DAG using a previously trained model"""
    dag_pred = DAG()

    # this special function adds a task with name "get" that will just forward
    # whatever value we pass when calling .build(). You can pass a function
    # in the "preprocessor" argument to perform arbitrary logic like parsing
    # or validation
    input_data_passer(dag=dag_pred, name="get", preprocessor=validate_input_data)

    # we re-use the same code that we used for training!
    add_features(dag_pred)

    # load model generated by the training graph
    with open(Path("output", "model.pickle"), "rb") as f:
        model = pickle.load(f)

    # add the final task, this special function just executes whatever
    # function we pass as the first argument, we can pass arbitrary parameters
    # using "params"
    predict_task = in_memory_callable(
        predict, dag=dag_pred, name="predict", params=dict(model=model)
    )

    # predict after joining features
    dag_pred["join"] >> predict_task

    # convert our batch-processing pipeline to a in-memory one and return
    return InMemoryDAG(dag_pred)


# instantiate training pipeline
dag = make_training()
# run it (generates model.pkl)
dag.build()

# instantiate prediction pipeline
dag_pred = make_predict()

# input data: generates features from this and then feeds the model
sample_input = pd.DataFrame(
    {
        "sepal length (cm)": [5.9],
        "sepal width (cm)": [3],
        "petal length (cm)": [5.1],
        "petal width (cm)": [1.8],
    }
)

# pass input data through the prediction pipeline. A pipeline might have
# multiple inputs, in our case it only has one. The format is:
# {task_name: input_data}
result = dag_pred.build({"get": sample_input})

# result is a dictionary with {task_name: output}. Get the output from the
# predict task
result["predict"]
