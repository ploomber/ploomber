import click


@click.group()
def cli():
    """Internal CLI"""
    pass


@cli.command()
@click.argument("runid")
@click.argument("reason")
def mark_failed(runid, reason):
    """Mark latest run as failed if passed something different than "none" """
    from ploomber.cloud.api import PloomberCloudAPI

    api = PloomberCloudAPI()
    api.run_failed(runid, reason)


if __name__ == "__main__":
    cli()
