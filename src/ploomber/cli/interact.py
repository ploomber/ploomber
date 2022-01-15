from IPython import start_ipython
from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import cli_endpoint
from ploomber.telemetry import telemetry
import datetime


@cli_endpoint
def main():
    start_time = datetime.datetime.now()
    parser = CustomParser(description='Call an entry point '
                          '(pipeline.yaml or dotted path to factory)',
                          prog='ploomber interact')
    with parser:
        # this command has no static args
        pass

    dag, _ = parser.load_from_entry_point_arg()

    try:
        dag.render()
    except Exception:
        print('Your dag failed to render, but you can still inspect the '
              'object to debug it.\n')

    end_time = datetime.datetime.now()
    telemetry.log_api("ploomber_interact",
                      total_runtime=str(end_time - start_time))

    # NOTE: do not use embed here, we must use start_ipython, see here:
    # https://github.com/ipython/ipython/issues/8918
    start_ipython(argv=[], user_ns={'dag': dag})
