from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import cli_endpoint


def _apply(dag, method_name, kwargs=None):
    kwargs = kwargs or {}

    for task in dag.values():

        try:
            method = getattr(task, method_name)
        except AttributeError:
            pass

        method(**kwargs)


@cli_endpoint
def main():
    parser = CustomParser(description='Manage scripts and notebooks')

    with parser:
        group = parser.add_mutually_exclusive_group()
        group.add_argument('--cell', '-c', help='Inject/remove cell')
        group.add_argument('--reformat', '-r', help='Change format')
        group.add_argument('--pair', '-p', help='Pair with ipynb files')
        group.add_argument('--sync',
                           '-s',
                           action='store_true',
                           help='Sync ipynb files')

    dag, args = parser.load_from_entry_point_arg()

    dag.render()

    # TODO: should --pair automatically edit .gitignore, or just print a msg?
    # TODO: add --hook (pre-commit hook to remove cells)
    # NOTE: maybe inject/remove should edit some project's metadata (not sure
    # if pipeline.yaml) or another file. so the Jupyter plug-in does not remove
    # the injected cell upon exiting

    if args.sync:
        # maybe its more efficient to pass all notebook paths at once?
        _apply(dag, 'sync')
    elif args.reformat:
        _apply(dag, 'reformat', dict(fmt=args.reformat))
    elif args.pair:
        _apply(dag, 'pair', dict(base_path=args.pair))
    elif args.cell == 'inject':
        # TODO: if paired notebooks, also inject there
        _apply(dag, 'save_injected_cell', dict())
    elif args.cell == 'remove':
        _apply(dag, 'remove_injected_cell', dict())
    else:
        raise ValueError('action must be inject, remove')
