# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.17.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # An example notebook
#
# Sharing how a notebook can work in combination with a Python package.
#
# Notes:
# - [`jupytext`](https://github.com/mwouts/jupytext) helps us automatically
# export the notebook through `pyproject.toml` configuration.
# This makes it easier to provide line-level comments within pull
# request reviews or to use the Python file independent of the notebook.
# - [`jupyterlab_code_formatter`](https://github.com/jupyterlab-contrib/jupyterlab_code_formatter)
# helps automatically format the notebook on-the-fly using black, isort, and
# more so that our notebook has a consistent and readable format
# (including as we develop it!). This extension is available through the
# browser-based Jupyter interface
# as a circular button near the top of each notebook.
# Clicking it applies the formatting to the notebook.

# show imports from the local package
from iceberg_bioimage import ImageAsset, ScanResult

# build a serializable canonical object in the notebook
ScanResult(
    source_uri="data/example.zarr",
    format_family="zarr",
    image_assets=[
        ImageAsset(
            uri="data/example.zarr",
            array_path="0",
            shape=[1, 256, 256],
            dtype="uint16",
        )
    ],
)
