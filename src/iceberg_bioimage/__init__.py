"""Public package interface for iceberg_bioimage."""

from .api import (
    ingest_scan_results_to_warehouse,
    ingest_stores_to_warehouse,
    join_profiles_with_scan_result,
    join_profiles_with_store,
    register_store,
    scan_store,
    summarize_scan_result,
    summarize_store,
)
from .integrations.catalog import (
    CatalogScanOptions,
    catalog_table_to_arrow,
    join_catalog_image_assets_with_profiles,
    list_catalog_tables,
    load_catalog_table,
)
from .integrations.cytomining import (
    export_catalog_to_cytomining_warehouse,
    export_profiles_to_cytomining_warehouse,
    export_scan_result_to_cytomining_warehouse,
    export_store_to_cytomining_warehouse,
    export_table_to_cytomining_warehouse,
    load_warehouse_manifest,
)
from .integrations.duckdb import (
    create_duckdb_connection,
    join_image_assets_with_profiles,
    query_metadata_table,
)
from .integrations.ome_arrow import create_ome_arrow, scan_ome_arrow
from .models.scan_result import (
    ContractValidationResult,
    CytominingWarehouseResult,
    DatasetSummary,
    ImageAsset,
    RegistrationResult,
    ScanResult,
    WarehouseIngestResult,
    WarehouseManifest,
    WarehouseTableManifestEntry,
    WarehouseValidationResult,
)
from .publishing.chunk_index import publish_chunk_index
from .publishing.image_assets import publish_image_assets
from .validation.contracts import (
    load_profile_column_aliases,
    validate_microscopy_profile_columns,
    validate_microscopy_profile_table,
    validate_warehouse_manifest,
)

__all__ = [
    "CatalogScanOptions",
    "ContractValidationResult",
    "CytominingWarehouseResult",
    "DatasetSummary",
    "ImageAsset",
    "RegistrationResult",
    "ScanResult",
    "WarehouseIngestResult",
    "WarehouseManifest",
    "WarehouseTableManifestEntry",
    "WarehouseValidationResult",
    "catalog_table_to_arrow",
    "create_duckdb_connection",
    "create_ome_arrow",
    "export_catalog_to_cytomining_warehouse",
    "export_profiles_to_cytomining_warehouse",
    "export_scan_result_to_cytomining_warehouse",
    "export_store_to_cytomining_warehouse",
    "export_table_to_cytomining_warehouse",
    "ingest_scan_results_to_warehouse",
    "ingest_stores_to_warehouse",
    "join_catalog_image_assets_with_profiles",
    "join_image_assets_with_profiles",
    "join_profiles_with_scan_result",
    "join_profiles_with_store",
    "list_catalog_tables",
    "load_catalog_table",
    "load_profile_column_aliases",
    "load_warehouse_manifest",
    "publish_chunk_index",
    "publish_image_assets",
    "query_metadata_table",
    "register_store",
    "scan_ome_arrow",
    "scan_store",
    "summarize_scan_result",
    "summarize_store",
    "validate_microscopy_profile_columns",
    "validate_microscopy_profile_table",
    "validate_warehouse_manifest",
]
