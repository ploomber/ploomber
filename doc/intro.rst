Your DAG is a contract
----------------------

Once your pipeline is to be taken to production, it should be expected to run
in a predictable and reliable way. When your code is taken to a production
environment, it will be natural to ask questions such as: what resources does
it need? where does it store output? where is the SQL script to pull the data?

Answers to these questions could be provided in documentation, but if anything
changes, the documentation most likely will become outdated, useless in the
best case, confusing in the worst.

Since a DAG object is a full specification of your pipeline, it can answer all those questions. Taking a look at our pipeline specification we can clearly see
that our pipeline uses a sqlite database, stores its output in a temporary
directory and the query to pull the data. Furthermore, the status method
provides a summary of the pipeline's current state:


The ``dag`` object should be treated as a contract and developers must adhere
to it. This simple, yet effective feature makes our pipeline transparent for
anyone looking to productionize our code (e.g. a production engineer) or even
a colleague who just started working on the project.
