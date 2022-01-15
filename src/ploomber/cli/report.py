from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import cli_endpoint
from ploomber.telemetry import telemetry
import datetime


@cli_endpoint
def main():
    start_time = datetime.datetime.now()
    parser = CustomParser(description='Make a pipeline report',
                          prog='ploomber report')
    with parser:
        parser.add_argument(
            '--output',
            '-o',
            help='Where to save the report, defaults to pipeline.html',
            default='pipeline.html')
    dag, args = parser.load_from_entry_point_arg()
    dag.to_markup(path=args.output)
    end_time = datetime.datetime.now()
    telemetry.log_api("ploomber_report",
                      total_runtime=str(end_time - start_time))
    print('Report saved at:', args.output)
