import pytest

from ploomber.dag import plot


@pytest.mark.parametrize(
    "pygraphviz_installed, backend, path, backend_output",
    [
        [True, None, None, "pygraphviz"],
        [True, None, "pipeline.html", "d3"],
        [True, "d3", "pipeline.html", "d3"],
        [True, "mermaid", "pipeline.html", "mermaid"],
    ],
)
def test_choose_backend(
    monkeypatch, pygraphviz_installed, backend, path, backend_output
):
    monkeypatch.setattr(
        plot, "check_pygraphviz_installed", lambda: pygraphviz_installed
    )
    assert plot.choose_backend(backend=backend, path=path) == backend_output
