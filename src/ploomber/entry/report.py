from ploomber.entry.parsers import _custom_command, CustomParser


def main():
    parser = CustomParser(description='Make a pipeline report')
    parser.add_argument('entry_point', help='Entry point (DAG)')
    parser.add_argument(
        '--output',
        '-o',
        help='Where to save the report, defaults to pipeline.html',
        default='pipeline.html')
    dag, args = _custom_command(parser)
    dag.to_markup(path=args.output)
