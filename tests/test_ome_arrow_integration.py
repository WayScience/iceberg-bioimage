"""Tests for optional OME-Arrow integration helpers (ome-arrow >= 0.0.10)."""

from __future__ import annotations

import importlib
import sys
from unittest.mock import MagicMock, patch

import pytest
from pytest import MonkeyPatch

from iceberg_bioimage.integrations.ome_arrow import (
    create_ome_arrow,
    create_ome_arrow_from_tiff,
    create_ome_arrow_from_zarr,
    open_ome_arrow_dataset,
    scan_ome_arrow,
    write_ome_arrow_dataset,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_missing_import(monkeypatch: MonkeyPatch) -> None:
    """Simulate ome_arrow not being installed."""
    monkeypatch.delitem(sys.modules, "ome_arrow", raising=False)
    original = importlib.import_module

    def _missing(name: str, package: str | None = None) -> object:
        if name == "ome_arrow":
            raise ImportError("ome_arrow is unavailable")
        return original(name, package)

    monkeypatch.setattr(importlib, "import_module", _missing)


# ---------------------------------------------------------------------------
# Missing-dependency guard (all entry points share the same guard)
# ---------------------------------------------------------------------------


def test_create_ome_arrow_requires_optional_dependency(
    monkeypatch: MonkeyPatch,
) -> None:
    _make_missing_import(monkeypatch)
    with pytest.raises(RuntimeError, match="optional ome-arrow extra"):
        create_ome_arrow("image.ome.tiff")


def test_scan_ome_arrow_requires_optional_dependency(
    monkeypatch: MonkeyPatch,
) -> None:
    _make_missing_import(monkeypatch)
    with pytest.raises(RuntimeError, match="optional ome-arrow extra"):
        scan_ome_arrow("image.ome.parquet")


def test_create_ome_arrow_from_zarr_requires_optional_dependency(
    monkeypatch: MonkeyPatch,
) -> None:
    _make_missing_import(monkeypatch)
    with pytest.raises(RuntimeError, match="optional ome-arrow extra"):
        create_ome_arrow_from_zarr("image.ome.zarr")


def test_create_ome_arrow_from_tiff_requires_optional_dependency(
    monkeypatch: MonkeyPatch,
) -> None:
    _make_missing_import(monkeypatch)
    with pytest.raises(RuntimeError, match="optional ome-arrow extra"):
        create_ome_arrow_from_tiff("image.ome.tiff")


def test_open_ome_arrow_dataset_requires_optional_dependency(
    monkeypatch: MonkeyPatch,
) -> None:
    _make_missing_import(monkeypatch)
    with pytest.raises(RuntimeError, match="optional ome-arrow extra"):
        open_ome_arrow_dataset("/data/dataset")


def test_write_ome_arrow_dataset_requires_optional_dependency(
    monkeypatch: MonkeyPatch,
) -> None:
    _make_missing_import(monkeypatch)
    with pytest.raises(RuntimeError, match="optional ome-arrow extra"):
        write_ome_arrow_dataset([], "/data/out")


# ---------------------------------------------------------------------------
# Delegation — verify each wrapper calls the right ome_arrow symbol
# ---------------------------------------------------------------------------


def test_create_ome_arrow_delegates_to_ome_arrow_class() -> None:
    fake_oa = MagicMock()
    with patch.dict("sys.modules", {"ome_arrow": fake_oa}):
        create_ome_arrow("image.ome.tiff", lazy=True)
    fake_oa.OMEArrow.assert_called_once_with(data="image.ome.tiff", lazy=True)


def test_scan_ome_arrow_delegates_to_scan_classmethod() -> None:
    fake_oa = MagicMock()
    with patch.dict("sys.modules", {"ome_arrow": fake_oa}):
        scan_ome_arrow("image.parquet", row_index=2)
    fake_oa.OMEArrow.scan.assert_called_once_with(data="image.parquet", row_index=2)


def test_create_ome_arrow_from_zarr_delegates_to_from_ome_zarr() -> None:
    fake_oa = MagicMock()
    with patch.dict("sys.modules", {"ome_arrow": fake_oa}):
        create_ome_arrow_from_zarr("plate.ome.zarr", image_id="plate-1")
    fake_oa.from_ome_zarr.assert_called_once_with("plate.ome.zarr", image_id="plate-1")


def test_create_ome_arrow_from_tiff_delegates_to_from_tiff() -> None:
    fake_oa = MagicMock()
    with patch.dict("sys.modules", {"ome_arrow": fake_oa}):
        create_ome_arrow_from_tiff("image.ome.tiff", clamp_to_uint16=False)
    fake_oa.from_tiff.assert_called_once_with(
        "image.ome.tiff", clamp_to_uint16=False
    )


def test_open_ome_arrow_dataset_delegates_to_ome_arrow_dataset_class() -> None:
    fake_oa = MagicMock()
    with patch.dict("sys.modules", {"ome_arrow": fake_oa}):
        open_ome_arrow_dataset("/data/my_dataset")
    fake_oa.OMEArrowDataset.assert_called_once_with("/data/my_dataset")


def test_write_ome_arrow_dataset_delegates_to_write_function() -> None:
    fake_oa = MagicMock()
    images = [MagicMock()]
    with patch.dict("sys.modules", {"ome_arrow": fake_oa}):
        write_ome_arrow_dataset(images, "/data/out", compression="zstd")
    fake_oa.write_ome_arrow_dataset.assert_called_once_with(
        images, "/data/out", compression="zstd"
    )
