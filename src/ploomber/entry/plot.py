import argparse


def main():
    parser = argparse.ArgumentParser(description='Plot a pipeline')
    parser.add_argument('entry_point', help='Entry point (DAG)')
    parser.add_argument('--path',
                        '-p',
                        help='Path to save the plot',
                        default='pipeline.png')
    parser.parse_args()
