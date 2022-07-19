from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import cli_endpoint
from ploomber_core.telemetry import telemetry
from ploomber import __version__ as ver


@cli_endpoint
@telemetry.log_call('status', 'ploomber', ver)
def main():
    parser = CustomParser(description='Show pipeline status',
                          prog='ploomber status')
    with parser:
        # this command has no static args
        pass

    dag, _ = parser.load_from_entry_point_arg()

    print(dag.status())
