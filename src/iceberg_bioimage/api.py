"""Public API entry points."""

from __future__ import annotations

from collections.abc import Sequence

from iceberg_bioimage.adapters.base import BaseAdapter
from iceberg_bioimage.adapters.ome_tiff import OMETiffAdapter
from iceberg_bioimage.adapters.zarr_v2 import ZarrV2Adapter
from iceberg_bioimage.models.scan_result import RegistrationResult, ScanResult
from iceberg_bioimage.publishing.chunk_index import publish_chunk_index
from iceberg_bioimage.publishing.image_assets import (
    SupportsCatalog,
    publish_image_assets,
)


def _build_adapters() -> Sequence[BaseAdapter]:
    return (ZarrV2Adapter(), OMETiffAdapter())


def scan_store(uri: str) -> ScanResult:
    """Scan a supported image store and return canonical metadata."""

    for adapter in _build_adapters():
        if adapter.can_handle(uri):
            return adapter.scan(uri)

    raise ValueError(
        "Unsupported bioimage URI "
        f"{uri!r}. Supported formats are .zarr, .tif, and .tiff."
    )


def register_store(
    uri: str,
    catalog: str | SupportsCatalog,
    namespace: str | Sequence[str],
    *,
    image_assets_table: str = "image_assets",
    chunk_index_table: str | None = "chunk_index",
) -> RegistrationResult:
    """Scan a store and publish canonical metadata tables."""

    scan_result = scan_store(uri)
    image_assets_rows = publish_image_assets(
        catalog,
        namespace,
        image_assets_table,
        scan_result,
    )
    chunk_rows = 0
    if chunk_index_table is not None:
        chunk_rows = publish_chunk_index(
            catalog,
            namespace,
            chunk_index_table,
            scan_result,
        )

    return RegistrationResult(
        source_uri=uri,
        image_assets_rows_published=image_assets_rows,
        chunk_rows_published=chunk_rows,
    )
