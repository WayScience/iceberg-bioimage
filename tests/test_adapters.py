"""Adapter-specific regression tests."""

from __future__ import annotations

from iceberg_bioimage.adapters.ome_tiff import OMETiffAdapter
from iceberg_bioimage.adapters.zarr_v2 import ZarrV2Adapter


def test_ome_tiff_image_id_strips_suffix_case_insensitively() -> None:
    adapter = OMETiffAdapter()

    assert adapter._image_id("/tmp/image.OME.TIFF", 0) == "image"


def test_zarr_image_id_strips_suffix_case_insensitively() -> None:
    adapter = ZarrV2Adapter()

    assert adapter._image_id("/tmp/data.ZARR", None) == "data"


def test_zarr_image_id_strips_ome_zarr_suffix() -> None:
    adapter = ZarrV2Adapter()

    assert adapter._image_id("/tmp/sample.OME.ZARR", None) == "sample"
