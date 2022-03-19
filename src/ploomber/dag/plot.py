import json
import jinja2
from ploomber import resources
try:
    import importlib.resources as importlib_resources
except ImportError:  # pragma: no cover
    # backported
    import importlib_resources


def json_dag_parser(graph_dict: dict):
    node_repr_json = {}
    for each_node in graph_dict["nodes"]:
        node_repr_json[each_node["id"]] = each_node

    # change name label to products for now
    for each_node in node_repr_json:
        node_repr_json[each_node]["products"] = (
            node_repr_json[each_node]["label"].replace("\n", "").split(","))

    for each_link in graph_dict["links"]:
        existing_nodeLinks = node_repr_json[each_link["source"]].get(
            "parentIds", [])
        existing_nodeLinks.append(each_link["target"])
        node_repr_json[each_link["source"]]["parentIds"] = existing_nodeLinks

    return json.dumps(list(node_repr_json.values()))


def with_d3(json_dag, output):
    """Generates D3 Dag html output and return output file name
    """
    template_html = jinja2.Template(
        importlib_resources.read_text(resources, 'dag_template.html'))

    # template = jinja2.Template(open("../resources/d3_dag/dag_template.html").read())
    updated = template_html.render(
        {"json_data": json_dag_parser(graph_dict=json_dag)})
    with open(output, "w") as f:
        f.write(updated)
    return output