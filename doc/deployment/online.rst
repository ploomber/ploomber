Online service (API)
====================

To encapsulate all your pipeline's logic for online predictions, use
:py:mod:`ploomber.OnlineDAG`. Once implemented, you can generate predictions
like this:

.. code-block:: python
    :class: text-editor
    :name: online-py

    from my_project import MyOnlineDAG

    # MyOnlineDAG is a subclass of OnlineDAG
    dag = MyOnlineDAG()
    dag.predict(input_data=input_data)

You can easily integrate an online DAG with any library such as Flask or gRPC.

The only requisite is that your feature generation code should be entirely
made of Python functions (i.e., :py:mod:`ploomber.tasks.PythonCallable`) tasks
with configured :ref:`serializer-and-unserializer`.


Composing online pipelines
**************************

To create an online DAG, list your feature tasks in a ``features.yaml`` and
use ``import_tasks_from`` in your training pipeline (``pipeline.yaml``).
Subclass :py:mod:`ploomber.OnlineDAG` to create a serving pipeline.

``OnlineDAG`` will take your tasks from ``features.yaml`` and create
new "input tasks" based on ``upstream`` references in yout feature tasks.

For example, if ``features.yaml`` has tasks ``a_feature`` and
``another_feature`` (see the diagram in the first section), and both obtain
their inputs from a task named ``get``; the source code may look like this:

.. code-block:: py
    :class: text-editor
    :name: features-py

    def a_feature(upstream):
        raw_data = upstream['get']
        # process raw_data to generate features...
        # return a_feature
        return df_a_feature
    
    def another_feature(upstream):
        raw_data = upstream['get']
        # process raw_data to generate features...
        # return another_feature
        return df_another_feature

Since ``features.yaml`` does not contain a task named ``get``, ``OnlineDAG``
automatically identifies it as an "input task". Finally, you must provide a
"terminal task", which is the last task in your online pipeline:

.. raw:: html

    <div class="mermaid">
    graph LR
        A[Input] --> B1[A feature] --> C[Terminal task]
        A --> B2[Another feature] --> C
        subgraph Feature engineering
        B1
        B2
        end

    </div>

To implement this, create a subclass of ``OnlineDAG`` and provide the path
to your ``features.yaml``, parameters for your terminal task and the terminal
task:

.. code-block:: py
    :class: text-editor
    :name: online-dag-py

    from ploomber import OnlineDAG

    # subclass OnlineDAG...
    class MyOnlineDAG(OnlineDAG):
        # and provide these three methods...

        # get_partial: returns a path to your feature tasks
        @staticmethod
        def get_partial():
            return 'tasks-features.yaml'

        # terminal_params: returns a dictionary with parameters for the terminal task
        @staticmethod
        def terminal_params():
            model = pickle.loads(resources.read_binary(ml_online, 'model.pickle'))
            return dict(model=model)

        # terminal_task: implementation of your terminal task
        @staticmethod
        def terminal_task(upstream, model):
            # receives all tasks with no downtream dependencies in
            # tasks-features.yaml
            a_feature = upstream['a_feature']
            another_feature = upstream['another_feature']
            X = pd.DataFrame({'a_feature': a_feature,
                              'anoter_feature': anoter_feature})
            return model.predict(X)


To call ``MyOnlineDAG``:

.. code-block:: python
    :class: text-editor
    :name: online-py

    from my_project import MyOnlineDAG

    dag = MyOnlineDAG()

    # pass parameters (one per input)
    prediction = dag.predict(get=input_data)


You can import and call ``MyOnlineDAG`` in any framework (e.g., Flask) to
expose your pipeline as an online service.


.. code-block:: python
    :class: text-editor
    :name: micro-service-py

    from flask import Flask, request, jsonify
    import pandas as pd

    from my_project import OnlineDAG

    # instantiate online dag
    dag = OnlineDAG()
    app = Flask(__name__)

    @app.route('/', methods=['POST'])
    def predict():
        request_data = request.get_json()
        # get JSON data and create a data frame with a single row
        input_data = pd.DataFrame(request_data, index=[0])
        # pass input data, argument per root node
        out = pipeline.predict(get=input_data)
        # return output from the terminal task
        return jsonify({'prediction': int(out['terminal'])})


Examples
********

`Click here <https://soopervisor.readthedocs.io/en/latest/tutorials/aws-lambda.html>`_ to
see a deployment example using AWS Lambda.

`Click here <https://github.com/ploomber/projects/tree/master/templates/ml-online>`_ to
see a complete sample project that trains a model and exposes an API via Flask.