.. _incremental-builds:

What are incremental builds?
----------------------------

When developing pipelines, we usually make small changes and want to see how the
final output looks like (e.g, add a feature to a model training pipeline).
Incremental builds allows us to skip redundant work by only executing tasks
whose source code has changed since the last execution. To do so, Ploomber
has to save Product's metadata. For :py:mod:`ploomber.products.File`, it creates
another file in the same location, for SQL products such as
:py:mod:`ploomber.products.SQLRelation`, a metadata backend is required, which
is configured using the ``client`` parameter.
