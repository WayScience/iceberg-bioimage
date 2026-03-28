"""Tests for adapter selection and canonical scanning."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import tifffile
import zarr

from iceberg_bioimage import scan_store


def test_scan_store_reads_zarr(tmp_path: Path) -> None:
    store_path = tmp_path / "plate.zarr"
    root = zarr.open_group(store_path, mode="w")
    data = np.arange(12, dtype=np.uint16).reshape(3, 4)
    root.create_dataset("0", shape=data.shape, data=data, chunks=(2, 2))

    scan = scan_store(str(store_path))

    assert scan.format_family == "zarr"
    assert scan.source_uri == str(store_path)
    assert len(scan.image_assets) == 1
    asset = scan.image_assets[0]
    assert asset.array_path == "0"
    assert asset.shape == [3, 4]
    assert asset.chunk_shape == [2, 2]
    assert asset.dtype == "uint16"


def test_scan_store_reads_tiff(tmp_path: Path) -> None:
    image_path = tmp_path / "cells.ome.tiff"
    tifffile.imwrite(image_path, np.zeros((2, 3, 4), dtype=np.uint8))

    scan = scan_store(str(image_path))

    assert scan.format_family == "ome-tiff"
    assert len(scan.image_assets) == 1
    asset = scan.image_assets[0]
    assert asset.shape == [2, 3, 4]
    assert asset.dtype == "uint8"
    assert asset.metadata["channel_count"] is None


def test_scan_store_rejects_unknown_format() -> None:
    try:
        scan_store("example.h5")
    except ValueError as exc:
        assert "Unsupported bioimage URI" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected scan_store to reject unsupported formats.")
