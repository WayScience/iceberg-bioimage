"""Public package interface for iceberg_bioimage."""

from .api import register_store, scan_store
from .integrations.catalog import (
    CatalogScanOptions,
    catalog_table_to_arrow,
    join_catalog_image_assets_with_profiles,
    load_catalog_table,
)
from .integrations.duckdb import (
    create_duckdb_connection,
    join_image_assets_with_profiles,
    query_metadata_table,
)
from .models.scan_result import (
    ContractValidationResult,
    ImageAsset,
    RegistrationResult,
    ScanResult,
)
from .publishing.chunk_index import publish_chunk_index
from .publishing.image_assets import publish_image_assets
from .validation.contracts import (
    validate_microscopy_profile_columns,
    validate_microscopy_profile_table,
)

__all__ = [
    "CatalogScanOptions",
    "ContractValidationResult",
    "ImageAsset",
    "RegistrationResult",
    "ScanResult",
    "catalog_table_to_arrow",
    "create_duckdb_connection",
    "join_catalog_image_assets_with_profiles",
    "join_image_assets_with_profiles",
    "load_catalog_table",
    "publish_chunk_index",
    "publish_image_assets",
    "query_metadata_table",
    "register_store",
    "scan_store",
    "validate_microscopy_profile_columns",
    "validate_microscopy_profile_table",
]
