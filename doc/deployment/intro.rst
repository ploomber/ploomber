Introduction
============

The two most common ways to deploy data pipelines are batch and online.
Ploomber supports both deployment options.

In batch, you obtain new data to make predictions and store them for later
use. This process usually happens on a schedule. For example, you may develop a
Machine Learning pipeline that runs every morning, predicts the probability
of user churn, and stores such probabilities in a database table.

Alternatively, you may deploy a pipeline as an online service and expose your
model as a REST API; users request predictions at any time by sending input
data.

Pipeline composition
********************

Before diving into deployment details, let's introduce the concept of
pipeline composition.

The only difference between a Machine Learning training pipeline and its serving
counterpart is what happens at the beginning and the end.


At **training** time, we obtain historical data, generate features, and train a
model:


.. raw:: html

    <div class="mermaid">
    graph LR
        A[Get historical data] --> B1[A feature] --> C[Train model]
        A --> B2[Another feature] --> C
        subgraph Feature engineering
        B1
        B2
        end

    </div>

At **serving** time, we obtain new data, generate features and make
predictions using a trained model:


.. raw:: html

    <div class="mermaid">
    graph LR
        A[Get new data] --> B1[A feature] --> C[Predict]
        A --> B2[Another feature] --> C
        subgraph Feature engineering
        B1
        B2
        end

    </div>

When the feature engineering process does not match,
`training-serving skew <https://ploomber.io/posts/train-serve-skew/>`_ arises.
Training-serving skew is one of the most common problems when deploying ML models. To fix it,
Ploomber allows you to compose pipelines: **write your
feature generation once and re-use it to organize your training and serving
pipelines**; this ensures that the feature engineering code matches precisely.
