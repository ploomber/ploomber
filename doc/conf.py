# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
from ploomber import __version__

sys.path.insert(0, os.path.abspath('.'))
import hooks  # noqa

# -- Project information -----------------------------------------------------

project = 'ploomber'
copyright = '2022, ploomber'
author = 'ploomber'
version = __version__
release = version

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.autosummary',
    'sphinx.ext.autosectionlabel',
    'nbsphinx',
    'sphinx_togglebutton',
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store', '.ipynb_checkpoints/']

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#

html_theme = 'basic'

# https://docs.readthedocs.io/en/stable/guides/adding-custom-css.html
html_style = 'css/custom-theme.css'

html_js_files = [
    'js/custom.js',
    'js/mermaid.min.js',
    'js/terminal.js',
]

html_css_files = ['css/mermaid-style.css', 'css/mermaid-layout.css']

pygments_style = 'monokai'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# landing page
html_additional_pages = {'videos': 'videos.html'}

# nbsphinx

# This is inserted only in pages that load generate from notebooks
nbsphinx_prolog = """
.. raw:: html

    <style>
        div.nbinput.container div.input_area {
            background: black;
        }
    </style>

    <script src="../_static/js/nbsphinx.js"></script>
"""

# disable require.js loading - giving an exception. we don't need this anyway
# since this is only required to display widgets
nbsphinx_requirejs_path = ''

autosummary_generate = True

html_show_copyright = False


def setup(app):
    app.connect('config-inited', hooks.config_init)
    app.connect("builder-inited", hooks.jinja_filters)
