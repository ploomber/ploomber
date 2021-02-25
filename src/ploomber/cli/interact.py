from IPython import start_ipython
from ploomber.cli.parsers import CustomParser, _custom_command
from ploomber.cli.io import cli_endpoint


@cli_endpoint
def main():
    parser = CustomParser(description='Call an entry point '
                          '(pipeline.yaml or dotted path to factory)')
    with parser:
        # this command has no static args
        pass

    dag, _ = _custom_command(parser)

    try:
        dag.render()
    except Exception:
        print('Your dag failed to render, but you can still inspect the '
              'object to debug it.\n')

    # NOTE: do not use embed here, we must use start_ipytho, see here:
    # https://github.com/ipython/ipython/issues/8918
    start_ipython(argv=[], user_ns={'dag': dag})
