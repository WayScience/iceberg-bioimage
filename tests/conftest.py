"""Shared pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def iris_nuclei_3d(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Download the ome-iris nuclei-3d dataset (tiny preset) once per session.

    Requires network access.  Tests using this fixture must be marked with
    ``@pytest.mark.network``.
    """

    try:
        from OME_IRIS import datasets
    except ImportError as exc:
        pytest.skip(f"ome-iris is not installed: {exc}")

    output_dir = tmp_path_factory.mktemp("iris_nuclei_3d")
    try:
        datasets.download("nuclei-3d", output_dir, preset="tiny", silent=True)
    except Exception as exc:
        pytest.skip(f"Could not download nuclei-3d dataset: {exc}")

    return output_dir


@pytest.fixture(scope="session")
def iris_nf1(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Download the ome-iris nf1-cellpainting-shrunken dataset (tiny) per session.

    Requires network access.  Tests using this fixture must be marked with
    ``@pytest.mark.network``.
    """

    try:
        from OME_IRIS import datasets
    except ImportError as exc:
        pytest.skip(f"ome-iris is not installed: {exc}")

    output_dir = tmp_path_factory.mktemp("iris_nf1")
    try:
        datasets.download("nf1", output_dir, preset="tiny", silent=True)
    except Exception as exc:
        pytest.skip(f"Could not download nf1 dataset: {exc}")

    return output_dir
