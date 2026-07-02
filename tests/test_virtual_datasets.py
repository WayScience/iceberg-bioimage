"""Tests for virtual dataset API (catalog-free Arrow table access)."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest
import tifffile
import zarr

from iceberg_bioimage import (
    ImageAsset,
    ScanResult,
    scan_as_arrow_table,
    scan_result_as_arrow_table,
)

# ---------------------------------------------------------------------------
# scan_result_as_arrow_table
# ---------------------------------------------------------------------------


def test_scan_result_as_arrow_table_returns_pa_table() -> None:
    scan_result = ScanResult(
        source_uri="/data/sample.ome.zarr",
        format_family="zarr",
        image_assets=[
            ImageAsset(
                uri="/data/sample.ome.zarr",
                shape=[2, 256, 256],
                dtype="uint16",
                array_path="0",
                image_id="sample:0",
            )
        ],
    )

    table = scan_result_as_arrow_table(scan_result)

    assert isinstance(table, pa.Table)
    assert len(table) == 1


def test_scan_result_as_arrow_table_columns() -> None:
    scan_result = ScanResult(
        source_uri="/data/sample.ome.zarr",
        format_family="zarr",
        image_assets=[
            ImageAsset(
                uri="/data/sample.ome.zarr",
                shape=[2, 256, 256],
                dtype="uint16",
                image_id="sample",
            )
        ],
    )

    table = scan_result_as_arrow_table(scan_result)

    assert "dataset_id" in table.schema.names
    assert "image_id" in table.schema.names
    assert "format_family" in table.schema.names
    assert "uri" in table.schema.names
    assert "dtype" in table.schema.names
    assert "shape_json" in table.schema.names


def test_scan_result_as_arrow_table_dataset_id_derived_from_uri() -> None:
    scan_result = ScanResult(
        source_uri="/data/my_plate.ome.zarr",
        format_family="zarr",
        image_assets=[
            ImageAsset(uri="/data/my_plate.ome.zarr", shape=[1, 64, 64], dtype="uint8")
        ],
    )

    table = scan_result_as_arrow_table(scan_result)

    assert table["dataset_id"][0].as_py() == "my_plate"


EXPECTED_ASSET_COUNT = 3


def test_scan_result_as_arrow_table_multiple_assets() -> None:
    scan_result = ScanResult(
        source_uri="/data/plate.zarr",
        format_family="zarr",
        image_assets=[
            ImageAsset(
                uri="/data/plate.zarr",
                shape=[64, 64],
                dtype="uint8",
                array_path="A/1",
            ),
            ImageAsset(
                uri="/data/plate.zarr",
                shape=[64, 64],
                dtype="uint8",
                array_path="A/2",
            ),
            ImageAsset(
                uri="/data/plate.zarr",
                shape=[64, 64],
                dtype="uint8",
                array_path="B/1",
            ),
        ],
    )

    table = scan_result_as_arrow_table(scan_result)

    assert len(table) == EXPECTED_ASSET_COUNT


def test_scan_result_as_arrow_table_shape_json_is_valid_json() -> None:
    scan_result = ScanResult(
        source_uri="/data/sample.zarr",
        format_family="zarr",
        image_assets=[
            ImageAsset(uri="/data/sample.zarr", shape=[3, 128, 128], dtype="float32")
        ],
    )

    table = scan_result_as_arrow_table(scan_result)

    shape_json = table["shape_json"][0].as_py()
    assert json.loads(shape_json) == [3, 128, 128]


# ---------------------------------------------------------------------------
# scan_as_arrow_table — local zarr
# ---------------------------------------------------------------------------


def test_scan_as_arrow_table_zarr(tmp_path: Path) -> None:
    store_path = tmp_path / "sample.zarr"
    zarr.open(str(store_path), mode="w", shape=(2, 64, 64), dtype="uint16")

    table = scan_as_arrow_table(str(store_path))

    assert isinstance(table, pa.Table)
    assert len(table) == 1
    assert table["format_family"][0].as_py() == "zarr"
    assert table["dataset_id"][0].as_py() == "sample"


def test_scan_as_arrow_table_ome_zarr(tmp_path: Path) -> None:
    store_path = tmp_path / "plate.ome.zarr"
    root = zarr.open_group(str(store_path), mode="w")
    root.create_dataset("0", shape=(1, 3, 64, 64), dtype="uint16")
    root.attrs["multiscales"] = [
        {
            "axes": [
                {"name": "t"},
                {"name": "c"},
                {"name": "y"},
                {"name": "x"},
            ],
            "datasets": [{"path": "0"}],
        }
    ]

    table = scan_as_arrow_table(str(store_path))

    assert isinstance(table, pa.Table)
    assert len(table) >= 1
    assert table["dataset_id"][0].as_py() == "plate"


def test_scan_as_arrow_table_ome_tiff(tmp_path: Path) -> None:
    tiff_path = tmp_path / "image.ome.tiff"
    data = np.zeros((2, 64, 64), dtype="uint16")
    tifffile.imwrite(str(tiff_path), data, photometric="minisblack")

    table = scan_as_arrow_table(str(tiff_path))

    assert isinstance(table, pa.Table)
    assert len(table) >= 1
    assert table["format_family"][0].as_py() == "ome-tiff"
    assert table["dataset_id"][0].as_py() == "image"


def test_scan_as_arrow_table_unsupported_uri_raises() -> None:
    with pytest.raises(ValueError, match="Unsupported bioimage URI"):
        scan_as_arrow_table("/tmp/not_an_image.csv")


# ---------------------------------------------------------------------------
# Virtual dataset: no catalog reference needed
# ---------------------------------------------------------------------------


def test_scan_as_arrow_table_no_catalog_needed(tmp_path: Path) -> None:
    """Arrow table is returned without any catalog or Iceberg setup."""
    store_path = tmp_path / "virtual.zarr"
    zarr.open(str(store_path), mode="w", shape=(4, 4), dtype="uint8")

    table = scan_as_arrow_table(str(store_path))

    # Verify the table is immediately queryable
    assert table.num_rows == 1
    assert table.schema.field("uri") is not None
