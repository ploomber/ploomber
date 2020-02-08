"""
Testing
=======

Testing is crucial for any data pipeline. Raw data comes from sources where
developers have no control over. Raw data inconsistencies are a common source
for bugs in data pipelines (e.g. a unusual high proportion of NAs in certain
column), but to make progress, a developer must make assumptions about the
input data and such assumptions must be tested every time the pipeline runs
to prevent errors from propagating to downstream tasks. Let's rewrite our
previous pipeline to implement this defensive programming approach:
"""
