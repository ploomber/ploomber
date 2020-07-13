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
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))
from ploomber import __version__


# -- Project information -----------------------------------------------------

project = 'ploomber'
copyright = '2020, ploomber'
author = 'ploomber'
version = __version__
release = version


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ['sphinx.ext.autodoc',
              'sphinx.ext.napoleon',
              'sphinx.ext.autosummary',
              # remove these two
              'IPython.sphinxext.ipython_console_highlighting',
              'IPython.sphinxext.ipython_directive',
              'sphinx_gallery.gen_gallery',
              'sphinx.ext.autosectionlabel',
              'nbsphinx']

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
# https://docs.readthedocs.io/en/stable/guides/adding-custom-css.html
html_style = 'css/custom-theme.css'

html_js_files = [
    'js/custom.js',
]

pygments_style = 'monokai'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# https://github.com/readthedocs/readthedocs.org/issues/2569
master_doc = 'index'

# sphinx-gallery

sphinx_gallery_conf = {
    'filename_pattern': '/',
    'ignore_pattern': r'__init__\.py',
}

nbsphinx_prolog = """
.. raw:: html

    <style>
        div.nbinput.container div.input_area {
            background: black;
        }
    </style>
"""
