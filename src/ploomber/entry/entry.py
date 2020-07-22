"""
The entry module automatically creates command line interfaces from functions
that return a DAG object.

Example:

Assume module.sub_module.entry_point is a function that returns a DAG.

Run this will parse the function parameters and return them:

    ``$ python -m ploomber.entry module.sub_module.entry_point --help``


If numpydoc is installed, it will parse the docstring (if any) and add it
when using --help

To build the dag returned by the entry point:

    ``$ python -m ploomber.entry module.sub_module.entry_point``


To start an interactive session by loading the entry point first:

    ``$ python -i -m ploomber.entry module.sub_module.entry_point --action status``

``ipython -i -m`` also works. Once the interactive session starts, the object
returned by the entry point will be available in the "dag" variable.

Features:

    * Parse docstring and show it using ``--help``
    * Enable logging to standard output using ``--log LEVEL``
    * Pass function parameters using ``--PARAM_NAME VALUE``
    * If the function is decorated with ``@with_env``, replace env variables using ``--env__KEY VALUE``


``ploomber.entry`` only works with the factory pattern (a function that returns
a dag). But this same pattern can be applied to start interactive sesssions
if you have your own CLI logic (note we are calling the module directly instead
of using ploomber.entry):

    ``$ python -i -m module.sub_module.entry_point --some_arg``


In IPython:

    ``$ ipython -i -m module.sub_module.entry_point -- --some_arg``


Note: you need to add ``--`` to prevent IPython from parsing your custom args

"""
from ploomber.entry.parsers import _custom_command, CustomParser


def _main():
    parser = CustomParser(description='Call an entry point '
                          '(pipeline.yaml or dotted path to factory)')
    parser.add_argument('entry_point', help='Entry point (DAG)')
    parser.add_argument('--action',
                        help='Action to execute, defaults to '
                        'build',
                        default='build')
    # TODO: should ignore action
    parser.add_argument('--partially',
                        '-p',
                        help='Build a pipeline partially until certain task',
                        default=None)

    dag, args = _custom_command(parser)

    if args.partially:
        dag.build_partially(args.partially)
    else:
        print(getattr(dag, args.action)())

    return dag
