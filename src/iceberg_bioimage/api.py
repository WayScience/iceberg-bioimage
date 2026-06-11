"""Public API entry points."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds

from iceberg_bioimage.adapters.base import BaseAdapter
from iceberg_bioimage.adapters.ome_tiff import OMETiffAdapter
from iceberg_bioimage.adapters.zarr_v2 import ZarrV2Adapter
from iceberg_bioimage.integrations.duckdb import (
    MetadataSource,
    join_image_assets_with_profiles,
)
from iceberg_bioimage.models.scan_result import (
    ContractValidationResult,
    DatasetSummary,
    RegistrationResult,
    ScanResult,
    WarehouseIngestResult,
)
from iceberg_bioimage.publishing.chunk_index import (
    delete_dataset_chunk_index,
    publish_chunk_index,
    scan_result_to_chunk_rows,
)
from iceberg_bioimage.publishing.image_assets import (
    SupportsCatalog,
    _dataset_id,
    delete_dataset_image_assets,
    publish_image_assets,
    publish_profile_table,
    scan_result_to_rows,
)
from iceberg_bioimage.validation.contracts import (
    validate_microscopy_profile_columns,
    validate_microscopy_profile_table,
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
    replace: bool = False,
) -> RegistrationResult:
    """Scan a store and publish canonical metadata tables.

    Parameters
    ----------
    replace:
        When ``True``, existing rows for this dataset are deleted before
        inserting the freshly-scanned metadata (upsert by ``dataset_id``).
        Requires the Iceberg table to support row-level deletes.
    """

    scan_result = scan_store(uri)
    if replace:
        _deregister_by_dataset_id(
            _dataset_id(scan_result.source_uri),
            catalog,
            namespace,
            image_assets_table,
            chunk_index_table,
        )
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
        source_uri=scan_result.source_uri,
        image_assets_rows_published=image_assets_rows,
        chunk_rows_published=chunk_rows,
    )


def deregister_store(
    uri: str,
    catalog: str | SupportsCatalog,
    namespace: str | Sequence[str],
    *,
    image_assets_table: str = "image_assets",
    chunk_index_table: str | None = "chunk_index",
) -> None:
    """Remove all catalog rows associated with a bioimage store.

    Derives the ``dataset_id`` from *uri* (same rule as :func:`register_store`)
    and deletes matching rows from ``image_assets`` and, optionally,
    ``chunk_index``.  If a table does not exist the call is silently skipped.
    """

    dataset_id = _dataset_id(uri)
    _deregister_by_dataset_id(
        dataset_id,
        catalog,
        namespace,
        image_assets_table,
        chunk_index_table,
    )


def register_directory(
    path: str,
    catalog: str | SupportsCatalog,
    namespace: str | Sequence[str],
    *,
    glob: str = "**/*.ome.zarr",
    image_assets_table: str = "image_assets",
    chunk_index_table: str | None = "chunk_index",
    replace: bool = False,
) -> WarehouseIngestResult:
    """Discover and register every matching dataset under *path*.

    Parameters
    ----------
    path:
        Root directory to search.
    glob:
        ``pathlib``-compatible glob pattern relative to *path*.
        Common values:

        * ``"**/*.ome.zarr"`` — OME-Zarr stores (default)
        * ``"**/*.zarr"``     — all Zarr stores (includes OME-Zarr)
        * ``"**/*.ome.tiff"`` — OME-TIFF files
        * ``"**/*.tif"``      — all TIFF files
    replace:
        Passed through to :func:`register_store` for each discovered dataset.
    """

    root = Path(path)
    uris = sorted(str(match) for match in root.glob(glob))

    datasets: list[RegistrationResult] = []
    all_warnings: list[str] = []
    normalized_namespace = _normalize_namespace_parts(namespace)

    for uri in uris:
        scan_result = scan_store(uri)
        if replace:
            _deregister_by_dataset_id(
                _dataset_id(scan_result.source_uri),
                catalog,
                normalized_namespace,
                image_assets_table,
                chunk_index_table,
            )
        image_assets_rows = publish_image_assets(
            catalog,
            normalized_namespace,
            image_assets_table,
            scan_result,
        )
        chunk_rows = 0
        if chunk_index_table is not None:
            chunk_rows = publish_chunk_index(
                catalog,
                normalized_namespace,
                chunk_index_table,
                scan_result,
            )
        datasets.append(
            RegistrationResult(
                source_uri=scan_result.source_uri,
                image_assets_rows_published=image_assets_rows,
                chunk_rows_published=chunk_rows,
            )
        )
        all_warnings.extend(scan_result.warnings)

    return WarehouseIngestResult(
        catalog=catalog if isinstance(catalog, str) else type(catalog).__name__,
        namespace=list(normalized_namespace),
        image_assets_table=image_assets_table,
        chunk_index_table=chunk_index_table,
        datasets=datasets,
        warnings=all_warnings,
    )


def _deregister_by_dataset_id(
    dataset_id: str,
    catalog: str | SupportsCatalog,
    namespace: str | Sequence[str],
    image_assets_table: str,
    chunk_index_table: str | None,
) -> None:
    delete_dataset_image_assets(catalog, namespace, image_assets_table, dataset_id)
    if chunk_index_table is not None:
        delete_dataset_chunk_index(catalog, namespace, chunk_index_table, dataset_id)


def ingest_scan_results_to_warehouse(
    scan_results: Sequence[ScanResult],
    catalog: str | SupportsCatalog,
    namespace: str | Sequence[str],
    *,
    image_assets_table: str = "image_assets",
    chunk_index_table: str | None = "chunk_index",
) -> WarehouseIngestResult:
    """Publish many scanned datasets into a Cytotable-compatible warehouse."""

    normalized_namespace = _normalize_namespace_parts(namespace)
    datasets: list[RegistrationResult] = []
    warnings: list[str] = []

    for scan_result in scan_results:
        image_assets_rows = publish_image_assets(
            catalog,
            normalized_namespace,
            image_assets_table,
            scan_result,
        )
        chunk_rows = 0
        if chunk_index_table is not None:
            chunk_rows = publish_chunk_index(
                catalog,
                normalized_namespace,
                chunk_index_table,
                scan_result,
            )

        datasets.append(
            RegistrationResult(
                source_uri=scan_result.source_uri,
                image_assets_rows_published=image_assets_rows,
                chunk_rows_published=chunk_rows,
            )
        )
        warnings.extend(scan_result.warnings)

    return WarehouseIngestResult(
        catalog=catalog if isinstance(catalog, str) else type(catalog).__name__,
        namespace=list(normalized_namespace),
        image_assets_table=image_assets_table,
        chunk_index_table=chunk_index_table,
        datasets=datasets,
        warnings=warnings,
    )


def ingest_stores_to_warehouse(
    uris: Sequence[str],
    catalog: str | SupportsCatalog,
    namespace: str | Sequence[str],
    *,
    image_assets_table: str = "image_assets",
    chunk_index_table: str | None = "chunk_index",
) -> WarehouseIngestResult:
    """Scan and publish many datasets into a Cytotable-compatible warehouse."""

    scan_results = [scan_store(uri) for uri in uris]
    return ingest_scan_results_to_warehouse(
        scan_results,
        catalog,
        namespace,
        image_assets_table=image_assets_table,
        chunk_index_table=chunk_index_table,
    )


def summarize_scan_result(scan_result: ScanResult) -> DatasetSummary:
    """Build a concise user-facing summary from a scan result."""

    axes = sorted(
        {
            str(asset.metadata["axes"])
            for asset in scan_result.image_assets
            if asset.metadata.get("axes")
        }
    )
    channel_counts = sorted(
        {
            int(asset.metadata["channel_count"])
            for asset in scan_result.image_assets
            if asset.metadata.get("channel_count") is not None
        }
    )
    storage_variants = sorted(
        {
            str(asset.metadata["storage_variant"])
            for asset in scan_result.image_assets
            if asset.metadata.get("storage_variant")
        }
    )
    shapes = sorted({tuple(asset.shape) for asset in scan_result.image_assets})

    return DatasetSummary(
        source_uri=scan_result.source_uri,
        format_family=scan_result.format_family,
        image_asset_count=len(scan_result.image_assets),
        chunked_asset_count=sum(
            1 for asset in scan_result.image_assets if asset.chunk_shape is not None
        ),
        array_paths=[
            asset.array_path if asset.array_path is not None else "<root>"
            for asset in scan_result.image_assets
        ],
        dtypes=sorted({asset.dtype for asset in scan_result.image_assets}),
        shapes=[list(shape) for shape in shapes],
        axes=axes,
        channel_counts=channel_counts,
        storage_variants=storage_variants,
        warnings=list(scan_result.warnings),
    )


def summarize_store(uri: str) -> DatasetSummary:
    """Scan a store and return a concise dataset summary."""

    return summarize_scan_result(scan_store(uri))


def join_profiles_with_scan_result(
    scan_result: ScanResult,
    profiles: MetadataSource,
    *,
    include_chunks: bool = False,
    profile_dataset_id: str | None = None,
) -> pa.Table:
    """Join canonical image assets from a scan result to profile rows.

    This helper uses the optional DuckDB integration at runtime. Install the
    `duckdb` extra/group before calling it.
    """

    validation = _validate_profiles(profiles)
    if (
        profile_dataset_id is not None
        and "dataset_id" in validation.missing_required_columns
    ):
        validation.missing_required_columns = [
            column
            for column in validation.missing_required_columns
            if column != "dataset_id"
        ]
    if not validation.is_valid:
        raise ValueError(
            "Profiles do not satisfy the microscopy join contract: "
            + ", ".join(validation.missing_required_columns)
        )

    image_assets = pa.Table.from_pylist(scan_result_to_rows(scan_result))
    chunk_index = None
    if include_chunks:
        chunk_index = pa.Table.from_pylist(scan_result_to_chunk_rows(scan_result))

    return join_image_assets_with_profiles(
        image_assets,
        profiles,
        chunk_index=chunk_index,
        profile_dataset_id=profile_dataset_id,
    )


def scan_result_as_arrow_table(scan_result: ScanResult) -> pa.Table:
    """Return image-asset metadata from a scan result as an Arrow table.

    No catalog or Iceberg tables are required — the table lives in memory and
    can be queried directly with DuckDB, PyArrow compute, or similar tools.
    """

    return pa.Table.from_pylist(scan_result_to_rows(scan_result))


def scan_as_arrow_table(uri: str) -> pa.Table:
    """Scan a supported image store and return metadata as an Arrow table.

    No catalog or Iceberg tables are required.  Drop an ome-zarr or ome-tiff
    path (local or remote) in, get a queryable Arrow table out.

    Supported URI schemes
    ---------------------
    * Local paths: ``/data/sample.ome.zarr``, ``./scan.tif``
    * file:// URLs: ``file:///data/sample.ome.zarr``
    * S3 (requires the ``s3`` optional dependency): ``s3://bucket/data.ome.zarr``
    * Any fsspec-compatible URL supported by the underlying adapter libraries.
    """

    return scan_result_as_arrow_table(scan_store(uri))


def join_profiles_with_store(
    uri: str,
    profiles: MetadataSource,
    *,
    include_chunks: bool = False,
    profile_dataset_id: str | None = None,
) -> pa.Table:
    """Scan a store and join its canonical image assets to profile rows.

    This helper uses the optional DuckDB integration at runtime. Install the
    `duckdb` extra/group before calling it.
    """

    return join_profiles_with_scan_result(
        scan_store(uri),
        profiles,
        include_chunks=include_chunks,
        profile_dataset_id=profile_dataset_id,
    )


def register_profile_table(
    path: str | Path,
    catalog: str | SupportsCatalog,
    namespace: str | Sequence[str],
    *,
    table_name: str = "profiles",
) -> int:
    """Publish a profile Parquet file into the Iceberg catalog.

    Profiles are conventionally stored under a ``profiles`` sub-namespace,
    e.g. pass ``namespace="nf1.profiles"`` for an NF1 experiment.

    Returns the number of rows published.
    """

    arrow_table = ds.dataset(str(path)).to_table()
    normalized_namespace = _normalize_namespace_parts(namespace)
    return publish_profile_table(arrow_table, catalog, normalized_namespace, table_name)


def _validate_profiles(profiles: MetadataSource) -> ContractValidationResult:
    if isinstance(profiles, (str, Path)):
        return validate_microscopy_profile_table(str(profiles))
    if isinstance(profiles, pa.Table):
        return validate_microscopy_profile_columns(list(profiles.schema.names))
    if isinstance(profiles, list):
        columns = sorted({key for row in profiles for key in row})
        return validate_microscopy_profile_columns(columns)

    raise TypeError(f"Unsupported profile source type: {type(profiles)!r}")


def _normalize_namespace_parts(namespace: str | Sequence[str]) -> tuple[str, ...]:
    if isinstance(namespace, str):
        return tuple(part for part in namespace.split(".") if part)

    return tuple(part for part in namespace if part)
