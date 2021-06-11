from ploomber.cli.parsers import _custom_command, CustomParser
from ploomber.cli.io import cli_endpoint
from ploomber.util.default import extract_name


@cli_endpoint
def main():
    parser = CustomParser(description='Plot a pipeline')
    with parser:
        parser.add_argument(
            '--output',
            '-o',
            help='Where to save the plot, defaults to pipeline.png',
            default=None)
    dag, args = _custom_command(parser)

    if args.output is not None:
        output = args.output
    else:
        name = extract_name(args.entry_point)
        output = 'pipeline.png' if name is None else f'pipeline.{name}.png'

    dag.plot(output=output)

    print('Plot saved at:', output)
