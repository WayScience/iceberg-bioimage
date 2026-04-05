"""Tests for image asset publishing helpers."""

from __future__ import annotations

import json

import pytest
from pytest import MonkeyPatch

from iceberg_bioimage.models.scan_result import ImageAsset, ScanResult
from iceberg_bioimage.publishing import image_assets as image_assets_module
from tests.fakes import FakeCatalog, FakeTable


def _stub_schema() -> object:
    return object()


def test_scan_result_to_rows() -> None:
    scan_result = ScanResult(
        source_uri="/tmp/experiment.zarr",
        format_family="zarr",
        image_assets=[
            ImageAsset(
                uri="/tmp/experiment.zarr",
                array_path="0",
                shape=[10, 20],
                dtype="uint16",
                chunk_shape=[5, 5],
                metadata={"channel_count": 2},
                image_id="experiment:0",
            )
        ],
    )

    rows = image_assets_module.scan_result_to_rows(scan_result)

    assert rows == [
        {
            "dataset_id": "experiment",
            "image_id": "experiment:0",
            "format_family": "zarr",
            "uri": "/tmp/experiment.zarr",
            "array_path": "0",
            "shape_json": "[10, 20]",
            "dtype": "uint16",
            "chunk_shape_json": "[5, 5]",
            "metadata_json": json.dumps({"channel_count": 2}, sort_keys=True),
        }
    ]


def test_publish_image_assets_creates_missing_table(
    monkeypatch: MonkeyPatch,
) -> None:
    scan_result = ScanResult(
        source_uri="/tmp/experiment.zarr",
        format_family="zarr",
        image_assets=[
            ImageAsset(
                uri="/tmp/experiment.zarr",
                shape=[10, 20],
                dtype="uint16",
            )
        ],
    )
    fake_catalog = FakeCatalog()

    monkeypatch.setattr(
        image_assets_module,
        "_build_image_assets_schema",
        _stub_schema,
    )

    with pytest.warns(UserWarning, match="CytoTable's expected Iceberg namespace"):
        row_count = image_assets_module.publish_image_assets(
            catalog=fake_catalog,
            namespace="bioimage.dev",
            table_name="image_assets",
            scan_result=scan_result,
        )

    assert row_count == 1
    assert fake_catalog.created_namespaces == [("bioimage", "dev", "cytotable")]
    assert fake_catalog.created_identifiers == [
        ("bioimage", "dev", "cytotable", "image_assets")
    ]
    assert fake_catalog.table is not None
    assert len(fake_catalog.table.appends) == 1


def test_publish_image_assets_uses_existing_legacy_table(
    monkeypatch: MonkeyPatch,
) -> None:
    scan_result = ScanResult(
        source_uri="/tmp/experiment.zarr",
        format_family="zarr",
        image_assets=[
            ImageAsset(
                uri="/tmp/experiment.zarr",
                shape=[10, 20],
                dtype="uint16",
            )
        ],
    )
    legacy_table = FakeTable()
    fake_catalog = FakeCatalog(tables={("bioimage", "image_assets"): legacy_table})

    monkeypatch.setattr(
        image_assets_module,
        "_build_image_assets_schema",
        _stub_schema,
    )

    with pytest.warns(UserWarning, match="Using 'bioimage.image_assets'"):
        row_count = image_assets_module.publish_image_assets(
            catalog=fake_catalog,
            namespace="bioimage",
            table_name="image_assets",
            scan_result=scan_result,
        )

    assert row_count == 1
    assert fake_catalog.created_identifiers == []
    assert len(legacy_table.appends) == 1
