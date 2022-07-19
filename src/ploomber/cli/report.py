from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import cli_endpoint
from ploomber.telemetry import telemetry


@cli_endpoint
@telemetry.log_call('report')
def main():
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

    print('Report saved at:', args.output)
