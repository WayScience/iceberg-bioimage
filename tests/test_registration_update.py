"""Tests for replace/upsert, deregister, and directory registration APIs."""

from __future__ import annotations

from pathlib import Path

import pytest
import zarr

from iceberg_bioimage import (
    deregister_store,
    register_directory,
    register_store,
)
from iceberg_bioimage.models.scan_result import ImageAsset, ScanResult
from tests.fakes import FakeCatalog, FakeTable

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

EXPECTED_DIRECTORY_DATASET_COUNT = 2

_SIMPLE_SCAN = ScanResult(
    source_uri="/tmp/plate.ome.zarr",
    format_family="zarr",
    image_assets=[
        ImageAsset(uri="/tmp/plate.ome.zarr", shape=[2, 64, 64], dtype="uint16")
    ],
)


def _fake_scan(uri: str) -> ScanResult:
    return _SIMPLE_SCAN


# ---------------------------------------------------------------------------
# register_store — replace=True (upsert)
# ---------------------------------------------------------------------------


def test_register_store_replace_deletes_before_append(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delete_calls: list[tuple[str, object]] = []

    def fake_delete(
        catalog: object,
        namespace: object,
        table_name: str,
        dataset_id: str,
    ) -> None:
        delete_calls.append((table_name, dataset_id))

    monkeypatch.setattr("iceberg_bioimage.api.scan_store", _fake_scan)
    monkeypatch.setattr(
        "iceberg_bioimage.api.delete_dataset_image_assets", fake_delete
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.delete_dataset_chunk_index", fake_delete
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_image_assets",
        lambda *a, **kw: 1,
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_chunk_index",
        lambda *a, **kw: 0,
    )

    register_store("/tmp/plate.ome.zarr", "default", "bio", replace=True)

    assert ("image_assets", "plate") in delete_calls
    assert ("chunk_index", "plate") in delete_calls


def test_register_store_replace_false_does_not_delete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delete_calls: list[object] = []

    monkeypatch.setattr("iceberg_bioimage.api.scan_store", _fake_scan)
    monkeypatch.setattr(
        "iceberg_bioimage.api.delete_dataset_image_assets",
        lambda *a, **kw: delete_calls.append(a),
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.delete_dataset_chunk_index",
        lambda *a, **kw: delete_calls.append(a),
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_image_assets",
        lambda *a, **kw: 1,
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_chunk_index",
        lambda *a, **kw: 0,
    )

    register_store("/tmp/plate.ome.zarr", "default", "bio", replace=False)

    assert delete_calls == []


def test_register_store_replace_skips_chunk_delete_when_no_chunk_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chunk_delete_calls: list[object] = []

    monkeypatch.setattr("iceberg_bioimage.api.scan_store", _fake_scan)
    monkeypatch.setattr(
        "iceberg_bioimage.api.delete_dataset_image_assets",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.delete_dataset_chunk_index",
        lambda *a, **kw: chunk_delete_calls.append(a),
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_image_assets",
        lambda *a, **kw: 1,
    )

    register_store(
        "/tmp/plate.ome.zarr",
        "default",
        "bio",
        replace=True,
        chunk_index_table=None,
    )

    assert chunk_delete_calls == []


# ---------------------------------------------------------------------------
# register_store — FakeTable integration (verifies delete → append order)
# ---------------------------------------------------------------------------


def test_register_store_replace_calls_delete_then_append(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = FakeTable()
    catalog = FakeCatalog(table=table)
    call_order: list[str] = []

    original_delete = table.delete
    original_append = table.append

    def tracking_delete(f: object) -> None:
        call_order.append("delete")
        original_delete(f)

    def tracking_append(t: object) -> None:
        call_order.append("append")
        original_append(t)

    table.delete = tracking_delete  # type: ignore[method-assign]
    table.append = tracking_append  # type: ignore[method-assign]

    monkeypatch.setattr("iceberg_bioimage.api.scan_store", _fake_scan)

    register_store(
        "/tmp/plate.ome.zarr",
        catalog,
        ("bio", "cytotable"),
        replace=True,
        chunk_index_table=None,
    )

    assert call_order == ["delete", "append"]


# ---------------------------------------------------------------------------
# deregister_store
# ---------------------------------------------------------------------------


def test_deregister_store_deletes_both_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    delete_calls: list[tuple[str, str]] = []

    def fake_delete(
        catalog: object,
        namespace: object,
        table_name: str,
        dataset_id: str,
    ) -> None:
        delete_calls.append((table_name, dataset_id))

    monkeypatch.setattr(
        "iceberg_bioimage.api.delete_dataset_image_assets", fake_delete
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.delete_dataset_chunk_index", fake_delete
    )

    deregister_store("/tmp/plate.ome.zarr", "default", "bio")

    assert ("image_assets", "plate") in delete_calls
    assert ("chunk_index", "plate") in delete_calls


def test_deregister_store_skips_chunk_table_when_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chunk_calls: list[object] = []

    monkeypatch.setattr(
        "iceberg_bioimage.api.delete_dataset_image_assets",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.delete_dataset_chunk_index",
        lambda *a, **kw: chunk_calls.append(a),
    )

    deregister_store(
        "/tmp/plate.ome.zarr",
        "default",
        "bio",
        chunk_index_table=None,
    )

    assert chunk_calls == []


def test_deregister_store_dataset_id_strips_extension(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen_ids: list[str] = []

    def capture_delete(
        catalog: object,
        namespace: object,
        table_name: object,
        dataset_id: str,
    ) -> None:
        seen_ids.append(dataset_id)

    monkeypatch.setattr(
        "iceberg_bioimage.api.delete_dataset_image_assets", capture_delete
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.delete_dataset_chunk_index", capture_delete
    )

    deregister_store("/data/my_plate.ome.zarr", "default", "bio")

    assert all(did == "my_plate" for did in seen_ids)


# ---------------------------------------------------------------------------
# delete_dataset_image_assets — publishing layer
# ---------------------------------------------------------------------------


def test_delete_dataset_image_assets_calls_table_delete() -> None:
    from iceberg_bioimage.publishing.image_assets import delete_dataset_image_assets

    table = FakeTable()
    catalog = FakeCatalog(
        tables={("bio", "cytotable", "image_assets"): table}
    )

    delete_dataset_image_assets(catalog, ("bio",), "image_assets", "plate")

    assert len(table.deletes) == 1


def test_delete_dataset_image_assets_noop_when_table_missing() -> None:
    from iceberg_bioimage.publishing.image_assets import delete_dataset_image_assets

    catalog = FakeCatalog()  # no tables

    # Should not raise
    delete_dataset_image_assets(catalog, ("bio",), "image_assets", "plate")


def test_delete_dataset_chunk_index_calls_table_delete() -> None:
    from iceberg_bioimage.publishing.chunk_index import delete_dataset_chunk_index

    table = FakeTable()
    catalog = FakeCatalog(
        tables={("bio", "cytotable", "chunk_index"): table}
    )

    delete_dataset_chunk_index(catalog, ("bio",), "chunk_index", "plate")

    assert len(table.deletes) == 1


# ---------------------------------------------------------------------------
# register_directory
# ---------------------------------------------------------------------------


def test_register_directory_discovers_matching_stores(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "a.ome.zarr").mkdir()
    (tmp_path / "b.ome.zarr").mkdir()
    zarr.open(str(tmp_path / "a.ome.zarr"), mode="w", shape=(2, 2), dtype="uint8")
    zarr.open(str(tmp_path / "b.ome.zarr"), mode="w", shape=(2, 2), dtype="uint8")

    registered: list[str] = []

    def fake_publish(
        catalog: object,
        namespace: object,
        table_name: object,
        scan_result: ScanResult,
    ) -> int:
        registered.append(scan_result.source_uri)
        return 1

    monkeypatch.setattr("iceberg_bioimage.api.publish_image_assets", fake_publish)
    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_chunk_index", lambda *a, **kw: 0
    )

    result = register_directory(str(tmp_path), "default", "bio")

    assert result.dataset_count == EXPECTED_DIRECTORY_DATASET_COUNT
    assert all(uri.endswith(".ome.zarr") for uri in registered)


def test_register_directory_empty_dir_returns_zero_datasets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_image_assets", lambda *a, **kw: 0
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_chunk_index", lambda *a, **kw: 0
    )

    result = register_directory(str(tmp_path), "default", "bio")

    assert result.dataset_count == 0


def test_register_directory_custom_glob_finds_tiff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import numpy as np
    import tifffile

    tifffile.imwrite(str(tmp_path / "img.ome.tiff"), np.zeros((4, 4), dtype="uint8"))

    registered: list[str] = []

    def fake_publish(
        catalog: object,
        namespace: object,
        table_name: object,
        scan_result: ScanResult,
    ) -> int:
        registered.append(scan_result.source_uri)
        return 1

    monkeypatch.setattr("iceberg_bioimage.api.publish_image_assets", fake_publish)
    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_chunk_index", lambda *a, **kw: 0
    )

    result = register_directory(
        str(tmp_path), "default", "bio", glob="**/*.ome.tiff"
    )

    assert result.dataset_count == 1
    assert registered[0].endswith("img.ome.tiff")


def test_register_directory_replace_calls_delete_per_dataset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "plate.ome.zarr").mkdir()
    zarr.open(
        str(tmp_path / "plate.ome.zarr"), mode="w", shape=(2, 2), dtype="uint8"
    )

    delete_calls: list[str] = []

    def fake_delete(
        catalog: object,
        namespace: object,
        table_name: str,
        dataset_id: str,
    ) -> None:
        delete_calls.append(dataset_id)

    monkeypatch.setattr(
        "iceberg_bioimage.api.delete_dataset_image_assets", fake_delete
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.delete_dataset_chunk_index", fake_delete
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_image_assets", lambda *a, **kw: 1
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_chunk_index", lambda *a, **kw: 0
    )

    register_directory(str(tmp_path), "default", "bio", replace=True)

    assert "plate" in delete_calls


def test_register_directory_result_carries_namespace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_image_assets", lambda *a, **kw: 0
    )
    monkeypatch.setattr(
        "iceberg_bioimage.api.publish_chunk_index", lambda *a, **kw: 0
    )

    result = register_directory(str(tmp_path), "default", "bioimage.cytotable")

    assert result.namespace == ["bioimage", "cytotable"]
