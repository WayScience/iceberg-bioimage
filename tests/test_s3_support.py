"""Tests for remote/S3 URI support in adapters and the virtual dataset API."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from iceberg_bioimage.adapters.ome_tiff import OMETiffAdapter, _is_remote_uri
from iceberg_bioimage.adapters.zarr_v2 import ZarrV2Adapter

# ---------------------------------------------------------------------------
# URI detection helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "uri,expected",
    [
        ("s3://bucket/data.ome.zarr", True),
        ("gs://bucket/data.zarr", True),
        ("https://example.com/data.tif", True),
        ("http://example.com/data.tiff", True),
        ("/local/path/data.zarr", False),
        ("file:///local/path/data.zarr", False),
        ("./relative/path.tif", False),
    ],
)
def test_is_remote_uri(uri: str, expected: bool) -> None:
    assert _is_remote_uri(uri) == expected


# ---------------------------------------------------------------------------
# ZarrV2Adapter — remote URI can_handle and routing
# ---------------------------------------------------------------------------


def test_zarr_adapter_can_handle_s3_uri() -> None:
    adapter = ZarrV2Adapter()

    assert adapter.can_handle("s3://bucket/data.ome.zarr") is True
    assert adapter.can_handle("s3://bucket/data.zarr") is True
    assert adapter.can_handle("gs://bucket/plate.zarr") is True


def test_zarr_adapter_s3_uri_is_not_local_v3() -> None:
    adapter = ZarrV2Adapter()

    # S3 URIs must NOT be classified as local v3 stores
    assert adapter._is_local_zarr_v3("s3://bucket/data.zarr") is False
    assert adapter._is_local_zarr_v3("gs://bucket/data.zarr") is False


def test_zarr_adapter_scan_delegates_to_zarr_open_for_s3(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ZarrV2Adapter.scan() must call zarr.open() (not local path logic) for S3."""
    import zarr

    fake_array = MagicMock()
    fake_array.shape = (2, 64, 64)
    fake_array.dtype = np.dtype("uint16")
    fake_array.chunks = (1, 64, 64)
    fake_array.attrs = {}

    opened_uris: list[str] = []

    def fake_zarr_open(uri: str, mode: str = "r") -> object:
        opened_uris.append(uri)
        return fake_array

    monkeypatch.setattr(zarr, "open", fake_zarr_open)

    adapter = ZarrV2Adapter()
    result = adapter.scan("s3://my-bucket/plate.ome.zarr")

    assert opened_uris == ["s3://my-bucket/plate.ome.zarr"]
    assert len(result.image_assets) == 1
    assert result.image_assets[0].dtype == "uint16"


# ---------------------------------------------------------------------------
# OMETiffAdapter — remote URI error when fsspec missing
# ---------------------------------------------------------------------------


def test_ome_tiff_adapter_can_handle_remote_tiff() -> None:
    adapter = OMETiffAdapter()

    assert adapter.can_handle("s3://bucket/image.tif") is True
    assert adapter.can_handle("https://host.example.com/scan.tiff") is True


def test_ome_tiff_adapter_remote_uri_raises_without_fsspec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import builtins

    real_import = builtins.__import__

    def mock_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "fsspec":
            raise ImportError("fsspec not available")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", mock_import)

    adapter = OMETiffAdapter()
    with pytest.raises(ImportError, match="fsspec is required"):
        adapter.scan("s3://bucket/image.tif")


def test_ome_tiff_adapter_remote_scan_uses_fsspec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_open_tiff should call fsspec.open() for remote URIs."""
    # Build a real minimal TIFF in memory to feed through the fake fsspec
    import io

    import numpy as np
    import tifffile

    buf = io.BytesIO()
    tifffile.imwrite(buf, np.zeros((4, 4), dtype="uint8"))
    buf.seek(0)

    fake_open_ctx = MagicMock()
    fake_open_ctx.__enter__ = MagicMock(return_value=buf)
    fake_open_ctx.__exit__ = MagicMock(return_value=False)
    fake_open_ctx.open = MagicMock(return_value=buf)

    fake_fsspec = MagicMock()
    fake_fsspec.open = MagicMock(return_value=fake_open_ctx)

    with patch.dict("sys.modules", {"fsspec": fake_fsspec}):
        adapter = OMETiffAdapter()
        result = adapter.scan("s3://bucket/image.tif")

    fake_fsspec.open.assert_called_once_with("s3://bucket/image.tif", "rb")
    assert len(result.image_assets) >= 1


# ---------------------------------------------------------------------------
# ZarrV2Adapter — fsspec memory store (integration-style, no real S3)
# ---------------------------------------------------------------------------


def test_zarr_adapter_scan_with_fsspec_memory_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Zarr can open memory:// stores; confirms the fsspec delegation path."""
    import zarr
    from zarr.storage import MemoryStore

    mem_store = MemoryStore()
    zarr.open(mem_store, mode="w", shape=(3, 32, 32), dtype="float32")

    opened: list[object] = []
    original_open = zarr.open

    def intercepting_open(uri: object, mode: str = "r") -> object:
        opened.append(uri)
        return original_open(mem_store, mode=mode)

    monkeypatch.setattr(zarr, "open", intercepting_open)

    adapter = ZarrV2Adapter()
    result = adapter.scan("memory://bucket/data.zarr")

    assert len(result.image_assets) == 1
    assert result.image_assets[0].dtype == "float32"
