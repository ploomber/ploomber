import subprocess
from ploomber.entry.parsers import CustomParser


def main():
    parser = CustomParser(description='Call an entry point '
                          '(pipeline.yaml or dotted path to factory)')
    args = parser.parse_args()

    subprocess.run([
        'ipython', '-i', '-m', 'ploomber.entry', '--', '--action', 'status',
        '--entry_point', args.entry_point
    ])
