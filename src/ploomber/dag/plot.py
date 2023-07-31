import os
import json
from importlib.util import find_spec
from pathlib import Path

import jinja2
from IPython.display import IFrame, HTML, display

from ploomber.util.util import remove_dir

try:
    import importlib.resources as importlib_resources
except ImportError:  # pragma: no cover
    # backported
    import importlib_resources

from ploomber import resources


def check_pygraphviz_installed():
    return find_spec("pygraphviz") is not None


def choose_backend(backend, path=None):
    """Determine which backend to use for plotting
    Temporarily disable pygraphviz for Python 3.10 on Windows
    """

    if (
        (not check_pygraphviz_installed() and backend is None)
        or (backend == "d3")
        or (backend is None and path and Path(path).suffix == ".html")
    ):
        return "d3"

    elif backend == "mermaid":
        return "mermaid"

    return "pygraphviz"


def json_dag_parser(graph: dict):
    """Format dag dict so d3 can understand it"""
    nodes = {}

    for task in graph["nodes"]:
        nodes[task["id"]] = task

    # change name label to products for now
    for node in nodes:
        nodes[node]["products"] = nodes[node]["label"].replace("\n", "").split(",")

    for link in graph["links"]:
        node_links = nodes[link["target"]].get("parentIds", [])
        node_links.append(link["source"])
        nodes[link["target"]]["parentIds"] = node_links

    return json.dumps(list(nodes.values()))


def with_d3(graph, output, image_only=False):
    """Generates D3 Dag html output and return output file name"""
    json_data = json_dag_parser(graph=graph)
    if image_only:
        Path(output).write_text(json_data)
    else:
        template = jinja2.Template(
            importlib_resources.read_text(resources, "dag_template_d3.html")
        )

        rendered = template.render(json_data=json_data)
        Path(output).write_text(rendered)


def with_mermaid(graph, output, image_only=False):
    """Generate mermaid diagram of DAG"""

    indent = "    "

    # Make an empty array of the lines of the mermaid diagram.
    # we will push nodes / links to this array and then flatten it at the end.
    diagram_markup_lines = []

    # If there are a large number of nodes in the DAG, make a vertical diagram
    diagram_direction = "LR" if len(graph["nodes"]) < 10 else "TD"

    diagram_markup_lines.append(f"graph {diagram_direction}")

    def pascall_case(s):
        p = "".join(x for x in s.title().replace("-", " ") if not x.isspace())
        return p

    # Add the nodes to the diagram
    for node in graph["nodes"]:
        node_id = pascall_case(node["id"])
        node_name = node["id"]
        diagram_markup_lines.append(f"{indent}{node_id}[{node_name}]")

    # Add the links
    for link in graph["links"]:
        target = pascall_case(link["target"])
        source = pascall_case(link["source"])
        diagram_markup_lines.append(f"{indent}{source} --> {target}")

    # Flatten array to markup string
    diagram_markup = "\n".join(diagram_markup_lines)

    if image_only:
        raise NotImplementedError("mermaid back end supports html output only")
    else:
        template = jinja2.Template(
            importlib_resources.read_text(resources, "dag_template_mermaid.html")
        )

        rendered = template.render(diagram_markup=diagram_markup)
        Path(output).write_text(rendered)


def embedded_html(path):
    # set output cell to hold 100% of the embedded content
    display(HTML("<style>.output.output_scroll { height: 100% }</style>"))

    # create a copy of the file to embed in a local
    # directory inorder to load it with iFrame

    embedded_assets_dir = "embedded-assets"
    clear_embedded_assets_dir(embedded_assets_dir)

    original_file = Path(path)
    local_file_copy = os.path.join(embedded_assets_dir, original_file.name)

    with open(local_file_copy, "w+") as html:
        copy_of_html = Path(path).read_text()
        html.write(copy_of_html)

    iframe = IFrame(src=local_file_copy, width="100%", height=600)

    return iframe


def clear_embedded_assets_dir(embedded_assets_dir):
    remove_dir(embedded_assets_dir)
    os.mkdir(embedded_assets_dir)
