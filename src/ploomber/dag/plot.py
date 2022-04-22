import json
from pathlib import Path
from importlib.util import find_spec

import jinja2
from IPython.display import HTML

try:
    import importlib.resources as importlib_resources
except ImportError:  # pragma: no cover
    # backported
    import importlib_resources

from ploomber import resources
from ploomber.util.util import requires


def check_pygraphviz_installed():
    return find_spec("pygraphviz") is not None


def choose_backend(backend):
    """Determine which backend to use for plotting
    """
    if ((not check_pygraphviz_installed() and backend is None)
            or (backend == 'd3')):
        return 'd3'
    else:
        return 'pygraphviz'


def json_dag_parser(graph: dict):
    """Format dag dict so d3 can understand it
    """
    nodes = {}

    for task in graph["nodes"]:
        nodes[task["id"]] = task

    # change name label to products for now
    for node in nodes:
        nodes[node]["products"] = (nodes[node]["label"].replace("\n",
                                                                "").split(","))

    for link in graph["links"]:
        node_links = nodes[link["target"]].get("parentIds", [])
        node_links.append(link["source"])
        nodes[link["target"]]["parentIds"] = node_links

    return json.dumps(list(nodes.values()))


def with_d3(graph, output):
    """Generates D3 Dag html output and return output file name
    """
    template = jinja2.Template(
        importlib_resources.read_text(resources, 'dag_template.html'))

    rendered = template.render(json_data=json_dag_parser(graph=graph))
    Path(output).write_text(rendered)


@requires(['requests_html', 'nest_asyncio'],
          name='embedded HTML with D3 backend',
          pip_names=['requests-html', 'nest_asyncio'])
def embedded_html(path):
    import asyncio
    import nest_asyncio
    nest_asyncio.apply()
    return asyncio.get_event_loop().run_until_complete(
        _embedded_html(path=path))


async def _embedded_html(path):
    # https://github.com/jupyter/nbclient/blob/1d629b2bed561fde521e6408e190a8159f117ddc/nbclient/util.py
    # https://github.com/jupyter/nbclient/blob/main/requirements.txt
    from requests_html import HTML as HTML_, AsyncHTMLSession

    session = AsyncHTMLSession()
    html = HTML_(html=Path(path).read_text(), session=session)
    await html.arender()
    # ensure we close the session, otherwise this will fail on windows
    await session.close()
    return HTML(data=html.html)
