"""Tests for optional OME-Arrow integration helpers."""

from __future__ import annotations

import pytest

from iceberg_bioimage.integrations.ome_arrow import create_ome_arrow, scan_ome_arrow


def test_create_ome_arrow_requires_optional_dependency() -> None:
    with pytest.raises(RuntimeError, match="optional ome-arrow extra"):
        create_ome_arrow("image.ome.tiff")


def test_scan_ome_arrow_requires_optional_dependency() -> None:
    with pytest.raises(RuntimeError, match="optional ome-arrow extra"):
        scan_ome_arrow("image.ome.parquet")
