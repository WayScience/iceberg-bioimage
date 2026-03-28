"""Catalog-facing helpers for reading canonical Iceberg metadata tables."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa

from iceberg_bioimage.integrations.duckdb import (
    DEFAULT_JOIN_KEYS,
    MetadataSource,
    join_image_assets_with_profiles,
)
from iceberg_bioimage.publishing.image_assets import (
    SupportsCatalog as SupportsLoadTableCatalog,
)
from iceberg_bioimage.publishing.image_assets import (
    _normalize_namespace,
    _resolve_catalog,
)


class SupportsIcebergScan(Protocol):
    """Protocol for pyiceberg scan objects."""

    def to_arrow(self) -> pa.Table:
        """Materialize the scan as an Arrow table."""


class SupportsIcebergTable(Protocol):
    """Protocol for pyiceberg table objects."""

    def scan(
        self,
        row_filter: str = "True",
        selected_fields: tuple[str, ...] = ("*",),
        case_sensitive: bool = True,
        snapshot_id: int | None = None,
        limit: int | None = None,
        ) -> SupportsIcebergScan:
        """Return a scan object for the current table."""


@dataclass(frozen=True, slots=True)
class CatalogScanOptions:
    """Options for scanning a catalog-backed metadata table."""

    columns: Sequence[str] | None = None
    where: str | None = None
    snapshot_id: int | None = None
    limit: int | None = None


def load_catalog_table(
    catalog: str | SupportsLoadTableCatalog,
    namespace: str | Sequence[str],
    table_name: str,
) -> SupportsIcebergTable:
    """Load a canonical metadata table from a catalog."""

    resolved_catalog = _resolve_catalog(catalog)
    identifier = (*_normalize_namespace(namespace), table_name)
    return resolved_catalog.load_table(identifier)


def catalog_table_to_arrow(
    catalog: str | SupportsLoadTableCatalog,
    namespace: str | Sequence[str],
    table_name: str,
    *,
    scan_options: CatalogScanOptions | None = None,
) -> pa.Table:
    """Load a catalog table into Arrow via PyIceberg."""

    options = CatalogScanOptions() if scan_options is None else scan_options
    table = load_catalog_table(catalog, namespace, table_name)
    scan = table.scan(
        row_filter="True" if options.where is None else options.where,
        selected_fields=(
            ("*",) if options.columns is None else tuple(options.columns)
        ),
        snapshot_id=options.snapshot_id,
        limit=options.limit,
    )
    return scan.to_arrow()


def join_catalog_image_assets_with_profiles(
    catalog: str | SupportsLoadTableCatalog,
    namespace: str | Sequence[str],
    profiles: MetadataSource,
    *,
    chunk_index_table: str | None = None,
    join_keys: Sequence[str] = DEFAULT_JOIN_KEYS,
) -> pa.Table:
    """Join catalog-backed image metadata to a profile table."""

    image_assets = catalog_table_to_arrow(
        catalog,
        namespace,
        "image_assets",
    )
    chunk_index = None
    if chunk_index_table is not None:
        chunk_index = catalog_table_to_arrow(
            catalog,
            namespace,
            chunk_index_table,
        )

    return join_image_assets_with_profiles(
        image_assets,
        profiles,
        join_keys=join_keys,
        chunk_index=chunk_index,
    )
