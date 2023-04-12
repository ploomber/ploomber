from IPython import start_ipython
from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import cli_endpoint
from ploomber.telemetry import telemetry


@cli_endpoint
@telemetry.log_call("interact")
def main():
    parser = CustomParser(
        description="Call an entry point " "(pipeline.yaml or dotted path to factory)",
        prog="ploomber interact",
    )
    with parser:
        # this command has no static args
        pass

    dag, _ = parser.load_from_entry_point_arg()

    try:
        dag.render()
    except Exception:
        err = (
            "Your dag failed to render, but you can still inspect the "
            "object to debug it.\n"
        )
        telemetry.log_api(
            "interact-error-render",
            metadata={
                "type": "dag_render_failed",
                "exception": err,
                "dag": dag,
            },
        )
        print(err)

    # NOTE: do not use embed here, we must use start_ipython, see here:
    # https://github.com/ipython/ipython/issues/8918
    start_ipython(argv=[], user_ns={"dag": dag})
