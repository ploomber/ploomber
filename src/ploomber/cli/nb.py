from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import cli_endpoint


def _call_in_source(dag, method_name, kwargs=None):
    kwargs = kwargs or {}

    for task in dag.values():

        try:
            method = getattr(task.source, method_name)
        except AttributeError:
            pass

        method(**kwargs)


@cli_endpoint
def main():
    parser = CustomParser(description='Manage scripts and notebooks')

    with parser:
        cell = parser.add_mutually_exclusive_group()
        cell.add_argument('--inject',
                          '-i',
                          action='store_true',
                          help='Inject cell')
        cell.add_argument('--remove',
                          '-r',
                          action='store_true',
                          help='Remove injected cell')
        parser.add_argument('--format', '-f', help='Change format')
        parser.add_argument('--pair', '-p', help='Pair with ipynb files')
        parser.add_argument('--sync',
                            '-s',
                            action='store_true',
                            help='Sync ipynb files')

    dag, args = parser.load_from_entry_point_arg()

    dag.render()

    # TODO: add --hook (pre-commit hook to remove cells)
    # NOTE: maybe inject/remove should edit some project's metadata (not sure
    # if pipeline.yaml) or another file. so the Jupyter plug-in does not remove
    # the injected cell upon exiting

    if args.format:
        _call_in_source(dag, 'format', dict(fmt=args.format))

    if args.inject:
        # TODO: if paired notebooks, also inject there
        _call_in_source(dag, 'save_injected_cell', dict())

    if args.remove:
        _call_in_source(dag, 'remove_injected_cell', dict())

    if args.sync:
        # maybe its more efficient to pass all notebook paths at once?
        _call_in_source(dag, 'sync')

    # can pair give trouble if we're reformatting?
    elif args.pair:
        # TODO: print a message suggesting to add the folder to .gitignore
        _call_in_source(dag, 'pair', dict(base_path=args.pair))
