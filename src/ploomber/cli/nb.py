from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import cli_endpoint


@cli_endpoint
def main():
    parser = CustomParser(description='Manage scripts and notebooks')

    with parser:
        group = parser.add_mutually_exclusive_group()
        group.add_argument('--cell', '-c', help='Inject/remove cell')
        group.add_argument('--reformat', '-r', help='Change format')

    dag, args = parser.load_from_entry_point_arg()

    dag.render()

    # TODO: add --pair path/to/paired (to pair all .py with .ipynb)
    # TODO: should --pair automatically edit .gitignore, or just print a msg?
    # TODO: add --sync (to sync .py and .ipynb)
    # TODO: add --hook (pre-commit hook to remove cells)
    # NOTE: maybe inject/remove should edit some project's metadata (not sure
    # if
    # pipeline.yaml) or another file. so the Jupyter plug-in does not remove
    # the injected cell upon exiting

    if args.reformat:
        for task in dag.values():
            try:
                task.reformat(fmt=args.reformat)
            except AttributeError:
                pass
    elif args.cell == 'inject':
        for task in dag.values():
            try:
                task.save_injected_cell()
            except AttributeError:
                pass
    elif args.cell == 'remove':
        for task in dag.values():
            try:
                task.remove_injected_cell()
            except AttributeError:
                pass
    else:
        raise ValueError('action must be inject, remove')
