from IPython import embed
from ploomber.cli.parsers import CustomParser, _custom_command


def main():
    parser = CustomParser(description='Call an entry point '
                          '(pipeline.yaml or dotted path to factory)')
    with parser:
        # this command has no static args
        pass

    dag, args = _custom_command(parser)
    print(dag.status())

    del parser
    del args

    embed(colors='neutral')
