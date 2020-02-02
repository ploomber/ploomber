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

What? Another workflow management tool?
---------------------------------------

Before starting this project, I wondered whether there would be already a tool
to cover my needs and found none. I already knew about Airflow (where ploomber
takes a lot of inspiration from), but when going through the documentation I found out it was not aligned with my goals and the setup seemed an overkill.

The closest project I could find in terms of goals was
`drake <https://github.com/ropensci/drake>`_, but 1) is an R tool and 2) lacked
of some features I consider critical for production systems.

As far as I know, there are no workflow management tools that make the notion
of products explicit, from my experience, this is a huge source of bugs: each
developer opens connections to their own resources and saves to external
systems in each task, which leads to unrealiable pipelines that break when
deployed in a new system. `Metaflow <https://metaflow.org/>`_ completely
removes control over products by automatically serializing all variable
on each task, but that seems an overabstraction to me.

Furthermore, a lot of tools bundle tons of extra features that are not needed
in early experimentation phases which greatly increases setup time and requires
users to go through long documentation. I believe some of these features
(such as scheduling) should be treated as separate problems, bundling them
together locks your project to specific frameworks. ploomber aims to provide
only the most important features and let important implementation details up
to the developer.

Fianlly, some tools add so much boilerplate code that for someone unfamiliar
with the tool, it will be hard to understand which parts are tool-specific
and which ones are the relevant data transformation parts. Production systems
often have strict requirements and ploomber aims to be as transparent as
possible: relevant code should be clear and should run correctly without
ploomber with the least amount of changes.
