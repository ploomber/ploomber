import jupytext
import pytest

from ploomber.sources.nb_utils import find_cell_with_tag, find_cell_with_tags

case_simple = """# %%
# %% tags=["a-tag"]
x = 1
"""

case_repeated = """# %%
# %% tags=["a-tag"]
x = 1

# %% tags=["a-tag"]
x = 2
"""

case_many_tags = """# %%
# %%

# %% tags=["some-tag", "a-tag"]
x = 2
"""


@pytest.mark.parametrize('nb, tag, source, index', [
    [case_simple, 'a-tag', 'x = 1', 1],
    [case_simple, 'another-tag', None, None],
    [case_repeated, 'a-tag', 'x = 1', 1],
    [case_many_tags, 'a-tag', 'x = 2', 2],
])
def test_find_cell_with_tag(nb, tag, source, index):
    nb_ = jupytext.reads(nb)

    cell, index_found = find_cell_with_tag(nb_, tag)

    if source:
        assert cell['source'] == source
    else:
        assert cell is None

    assert index_found == index


@pytest.mark.parametrize('nb, tags, keys', [
    [case_simple, ['a-tag'], {'a-tag'}],
    [case_simple, ['another-tag'], set()],
    [case_repeated, ['a-tag'], {'a-tag'}],
    [case_many_tags, ['a-tag'], {'a-tag'}],
    [case_many_tags, ['a-tag', 'non-existing-tag'], {'a-tag'}],
])
def test_find_cell_with_tags(nb, tags, keys):
    nb_ = jupytext.reads(nb)

    found = find_cell_with_tags(nb_, tags)

    assert set(found) == keys
