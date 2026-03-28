"""Tests for the high-level registration workflow."""

from __future__ import annotations

from pytest import MonkeyPatch

from iceberg_bioimage.api import register_store
from iceberg_bioimage.models.scan_result import ImageAsset, ScanResult


def test_register_store(monkeypatch: MonkeyPatch) -> None:
    scan_result = ScanResult(
        source_uri="/tmp/example.zarr",
        format_family="zarr",
        image_assets=[
            ImageAsset(
                uri="/tmp/example.zarr",
                shape=[4, 4],
                dtype="uint16",
                chunk_shape=[2, 2],
            )
        ],
    )

    monkeypatch.setattr(
        "iceberg_bioimage.api.scan_store",
        lambda uri: scan_result,
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_image_assets",
        lambda catalog, namespace, table_name, scan: 1,
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_chunk_index",
        lambda catalog, namespace, table_name, scan: 4,
    )

    result = register_store(
        "/tmp/example.zarr",
        "default",
        "bioimage",
    )

    assert result.to_dict() == {
        "source_uri": "/tmp/example.zarr",
        "image_assets_rows_published": 1,
        "chunk_rows_published": 4,
    }


def test_register_store_uses_canonical_source_uri(monkeypatch: MonkeyPatch) -> None:
    scan_result = ScanResult(
        source_uri="/tmp/CANONICAL.zarr",
        format_family="zarr",
        image_assets=[
            ImageAsset(
                uri="/tmp/CANONICAL.zarr",
                shape=[4, 4],
                dtype="uint16",
            )
        ],
    )

    monkeypatch.setattr(
        "iceberg_bioimage.api.scan_store",
        lambda uri: scan_result,
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_image_assets",
        lambda catalog, namespace, table_name, scan: 1,
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_chunk_index",
        lambda catalog, namespace, table_name, scan: 0,
    )

    result = register_store(
        "/tmp/user-input.zarr",
        "default",
        "bioimage",
    )

    assert result.source_uri == "/tmp/CANONICAL.zarr"


def test_register_store_without_chunks(monkeypatch: MonkeyPatch) -> None:
    scan_result = ScanResult(
        source_uri="/tmp/example.zarr",
        format_family="zarr",
        image_assets=[
            ImageAsset(
                uri="/tmp/example.zarr",
                shape=[4, 4],
                dtype="uint16",
            )
        ],
    )

    monkeypatch.setattr(
        "iceberg_bioimage.api.scan_store",
        lambda uri: scan_result,
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_image_assets",
        lambda catalog, namespace, table_name, scan: 1,
    )

    def _forbid_chunk_publish(*args: object, **_kwargs: object) -> int:
        raise AssertionError("publish_chunk_index must not be called")

    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_chunk_index",
        _forbid_chunk_publish,
    )

    result = register_store(
        "/tmp/example.zarr",
        "default",
        "bioimage",
        chunk_index_table=None,
    )

    assert result.chunk_rows_published == 0
