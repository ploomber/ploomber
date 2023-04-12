from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import cli_endpoint
from ploomber.telemetry import telemetry


@cli_endpoint
@telemetry.log_call("status")
def main():
    parser = CustomParser(description="Show pipeline status", prog="ploomber status")
    with parser:
        # this command has no static args
        pass

    dag, _ = parser.load_from_entry_point_arg()

    print(dag.status())
