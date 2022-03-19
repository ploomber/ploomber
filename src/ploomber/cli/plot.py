from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import cli_endpoint
from ploomber.util.default import extract_name

from ploomber.telemetry import telemetry


@cli_endpoint
@telemetry.log_call('plot')
def main():
    parser = CustomParser(description='Plot a pipeline', prog='ploomber plot')
    with parser:
        parser.add_argument(
            '--output',
            '-o',
            help='Where to save the plot, defaults to pipeline.png',
            default=None)

        parser.add_argument(
            '--backend',
            '-b',
            help=
            'Which backend to use pygraphviz or d3, defaults to pygraphviz',
            default=None)

        parser.add_argument('--include-products',
                            '-i',
                            help='Include task products',
                            action='store_true')

    dag, args = parser.load_from_entry_point_arg()

    if args.output is not None:
        output = args.output
    else:
        name = extract_name(args.entry_point)
        output = 'pipeline.png' if name is None else f'pipeline.{name}.png'

    plot_output = dag.plot(output=output,
                           backend=args.backend,
                           include_products=args.include_products)

    if plot_output:
        print('Plot saved at:', plot_output)
    else:
        print('Plot saved at:', output)
