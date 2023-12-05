# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------

project = 'rework'
copyright = '2018-2023, Pythonian'
author = 'Pythonian'
release = '0.16.0'


# -- General configuration ---------------------------------------------------
#extensions = ['sphinx.ext.autodoc', 'autoapi.extension']
autoapi_dirs = ['../rework']

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

