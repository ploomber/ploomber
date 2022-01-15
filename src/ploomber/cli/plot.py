from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import cli_endpoint
from ploomber.util.default import extract_name
import datetime

from ploomber.telemetry import telemetry


@cli_endpoint
def main():
    start_time = datetime.datetime.now()
    parser = CustomParser(description='Plot a pipeline', prog='ploomber plot')
    with parser:
        parser.add_argument(
            '--output',
            '-o',
            help='Where to save the plot, defaults to pipeline.png',
            default=None)
    dag, args = parser.load_from_entry_point_arg()

    if args.output is not None:
        output = args.output
    else:
        name = extract_name(args.entry_point)
        output = 'pipeline.png' if name is None else f'pipeline.{name}.png'

    dag.plot(output=output)
    end_time = datetime.datetime.now()
    telemetry.log_api("ploomber_plot",
                      total_runtime=str(end_time - start_time))
    print('Plot saved at:', output)
