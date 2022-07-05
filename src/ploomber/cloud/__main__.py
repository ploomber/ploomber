import click

from ploomber.cloud import api


@click.group()
def cli():
    """Internal CLI
    """
    pass


@cli.command()
@click.argument('reason')
def mark_failed(reason):
    """Mark latest run as failed if passed something different than "none"
    """
    api.run_latest_failed(reason)


if __name__ == '__main__':
    cli()
