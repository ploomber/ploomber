import argparse

from ploomber.entry.parsers import _custom_command


def main():
    parser = argparse.ArgumentParser(description='Plot a pipeline')
    parser.add_argument('entry_point', help='Entry point (DAG)')
    parser.add_argument(
        '--output',
        '-o',
        help='Where to save the plot, defaults to pipeline.png',
        default='pipeline.png')
    parser.add_argument('--log',
                        help='Enables logging to stdout at the '
                        'specified level',
                        default=None)
    dag, args = _custom_command(parser, static_args=['entry_point', 'output'])
    dag.plot(output=args.output)
