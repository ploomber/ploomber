import subprocess
from ploomber.cli.parsers import CustomParser


def main():
    parser = CustomParser(description='Call an entry point '
                          '(pipeline.yaml or dotted path to factory)')
    args = parser.parse_args()

    subprocess.run([
        'ipython', '-i', '-m', 'ploomber.cli', '--', '--entry-point',
        args.entry_point
    ])
