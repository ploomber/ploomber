from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import cli_endpoint


@cli_endpoint
def main():
    parser = CustomParser(description="Show pipeline status", prog="ploomber status")
    with parser:
        # this command has no static args
        pass

    dag, _ = parser.load_from_entry_point_arg()

    print(dag.status())
