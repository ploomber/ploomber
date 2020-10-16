from IPython import embed
from ploomber.cli.parsers import CustomParser, _custom_command


def main():
    parser = CustomParser(description='Call an entry point '
                          '(pipeline.yaml or dotted path to factory)')
    with parser:
        # this command has no static args
        pass

    dag, args = _custom_command(parser)

    try:
        dag.render()
    except Exception:
        print('Your dag failed to render, but you can still inspect the '
              'object to debug it.\n')

    del parser
    del args

    embed(colors='neutral')
