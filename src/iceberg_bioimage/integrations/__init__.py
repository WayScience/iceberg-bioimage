"""Optional integration helpers."""

from .catalog import (
    CatalogScanOptions,
    catalog_table_to_arrow,
    join_catalog_image_assets_with_profiles,
    load_catalog_table,
)
from .duckdb import (
    create_duckdb_connection,
    join_image_assets_with_profiles,
    query_metadata_table,
)

__all__ = [
    "CatalogScanOptions",
    "catalog_table_to_arrow",
    "create_duckdb_connection",
    "join_catalog_image_assets_with_profiles",
    "join_image_assets_with_profiles",
    "load_catalog_table",
    "query_metadata_table",
]
