"""Tests for chunk index publishing helpers."""

from __future__ import annotations

from _pytest.monkeypatch import MonkeyPatch

from iceberg_bioimage.models.scan_result import ImageAsset, ScanResult
from iceberg_bioimage.publishing import chunk_index as chunk_index_module
from tests.test_publishing import FakeCatalog

EXPECTED_CHUNK_ROW_COUNT = 4


def test_scan_result_to_chunk_rows() -> None:
    scan_result = ScanResult(
        source_uri="/tmp/experiment.zarr",
        format_family="zarr",
        image_assets=[
            ImageAsset(
                uri="/tmp/experiment.zarr",
                array_path="0",
                shape=[5, 4],
                dtype="uint16",
                chunk_shape=[2, 3],
                image_id="experiment:0",
            )
        ],
    )

    rows = chunk_index_module.scan_result_to_chunk_rows(scan_result)

    assert rows == [
        {
            "dataset_id": "experiment",
            "image_id": "experiment:0",
            "array_path": "0",
            "chunk_key": "0/0",
            "chunk_coords_json": "[0, 0]",
            "byte_length": 12,
        },
        {
            "dataset_id": "experiment",
            "image_id": "experiment:0",
            "array_path": "0",
            "chunk_key": "0/1",
            "chunk_coords_json": "[0, 1]",
            "byte_length": 4,
        },
        {
            "dataset_id": "experiment",
            "image_id": "experiment:0",
            "array_path": "0",
            "chunk_key": "1/0",
            "chunk_coords_json": "[1, 0]",
            "byte_length": 12,
        },
        {
            "dataset_id": "experiment",
            "image_id": "experiment:0",
            "array_path": "0",
            "chunk_key": "1/1",
            "chunk_coords_json": "[1, 1]",
            "byte_length": 4,
        },
        {
            "dataset_id": "experiment",
            "image_id": "experiment:0",
            "array_path": "0",
            "chunk_key": "2/0",
            "chunk_coords_json": "[2, 0]",
            "byte_length": 6,
        },
        {
            "dataset_id": "experiment",
            "image_id": "experiment:0",
            "array_path": "0",
            "chunk_key": "2/1",
            "chunk_coords_json": "[2, 1]",
            "byte_length": 2,
        },
    ]


def test_publish_chunk_index_creates_missing_table(
    monkeypatch: MonkeyPatch,
) -> None:
    scan_result = ScanResult(
        source_uri="/tmp/experiment.zarr",
        format_family="zarr",
        image_assets=[
            ImageAsset(
                uri="/tmp/experiment.zarr",
                shape=[4, 4],
                dtype="uint16",
                chunk_shape=[2, 2],
            )
        ],
    )
    fake_catalog = FakeCatalog()

    monkeypatch.setattr(
        chunk_index_module,
        "_build_chunk_index_schema",
        lambda: object(),
    )

    row_count = chunk_index_module.publish_chunk_index(
        catalog=fake_catalog,
        namespace="bioimage.dev",
        table_name="chunk_index",
        scan_result=scan_result,
    )

    assert row_count == EXPECTED_CHUNK_ROW_COUNT
    assert fake_catalog.created_identifiers == [("bioimage", "dev", "chunk_index")]
    assert fake_catalog.table is not None
    assert len(fake_catalog.table.appends) == 1
