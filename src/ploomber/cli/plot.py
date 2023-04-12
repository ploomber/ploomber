import click

from ploomber.cli.parsers import CustomParser
from ploomber.cli.io import cli_endpoint
from ploomber.util.default import extract_name
from ploomber.dag.plot import choose_backend
from ploomber.telemetry import telemetry


@cli_endpoint
@telemetry.log_call("plot")
def main():
    parser = CustomParser(description="Plot a pipeline", prog="ploomber plot")
    with parser:
        parser.add_argument(
            "--output",
            "-o",
            help="Where to save the plot, defaults to pipeline.png",
            default=None,
        )

        parser.add_argument(
            "--backend",
            "-b",
            help=(
                "How to generate the plot: d3 or "
                "pygraphviz. Using whatever is "
                "available by default"
            ),
            default=None,
        )

        parser.add_argument(
            "--include-products",
            "-i",
            help="Include task products",
            action="store_true",
        )

    dag, args = parser.load_from_entry_point_arg()

    if args.output is not None:
        output = args.output
    else:
        name = extract_name(args.entry_point)
        ext = "png" if choose_backend(args.backend) == "pygraphviz" else "html"
        output = f"pipeline.{ext}" if name is None else f"pipeline.{name}.{ext}"

    dag.plot(
        output=output, backend=args.backend, include_products=args.include_products
    )

    click.echo(f"Plot saved at: {output}")
