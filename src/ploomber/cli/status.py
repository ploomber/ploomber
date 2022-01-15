from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import cli_endpoint
from ploomber.telemetry import telemetry
import datetime


@cli_endpoint
def main():
    start_time = datetime.datetime.now()
    parser = CustomParser(description='Show pipeline status',
                          prog='ploomber status')
    with parser:
        # this command has no static args
        pass
    dag, args = parser.load_from_entry_point_arg()
    print(dag.status())
    end_time = datetime.datetime.now()
    telemetry.log_api("ploomber_status",
                      total_runtime=str(end_time - start_time))
