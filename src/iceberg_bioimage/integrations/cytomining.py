"""Helpers for exporting Parquet-backed Cytomining warehouse layouts."""

from __future__ import annotations

import json
import shutil
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Literal

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from iceberg_bioimage.integrations.catalog import SupportsScanCatalog
from iceberg_bioimage.integrations.duckdb import MetadataSource
from iceberg_bioimage.models.scan_result import (
    CytominingWarehouseResult,
    ScanResult,
    WarehouseManifest,
    WarehouseTableManifestEntry,
)
from iceberg_bioimage.publishing.chunk_index import scan_result_to_chunk_rows
from iceberg_bioimage.publishing.image_assets import scan_result_to_rows
from iceberg_bioimage.validation.contracts import resolve_microscopy_profile_columns

WriteMode = Literal["overwrite", "append"]
DEFAULT_PROFILE_NAMESPACE = "profiles"
DEFAULT_IMAGE_NAMESPACE = "images"
DEFAULT_JOINED_PROFILES_TABLE = f"{DEFAULT_PROFILE_NAMESPACE}.joined_profiles"
DEFAULT_IMAGE_ASSETS_TABLE = f"{DEFAULT_IMAGE_NAMESPACE}.image_assets"
DEFAULT_CHUNK_INDEX_TABLE = f"{DEFAULT_IMAGE_NAMESPACE}.chunk_index"


def export_scan_result_to_cytomining_warehouse(  # noqa: PLR0913
    scan_result: ScanResult,
    warehouse_root: str | Path,
    *,
    profiles: MetadataSource | None = None,
    include_chunks: bool = True,
    image_assets_table_name: str = DEFAULT_IMAGE_ASSETS_TABLE,
    chunk_index_table_name: str = DEFAULT_CHUNK_INDEX_TABLE,
    joined_table_name: str = DEFAULT_JOINED_PROFILES_TABLE,
    profile_dataset_id: str | None = None,
    mode: WriteMode = "overwrite",
) -> CytominingWarehouseResult:
    """Write scan-derived metadata into a Parquet-backed Cytomining warehouse."""

    from iceberg_bioimage.api import join_profiles_with_scan_result

    root = Path(warehouse_root)
    image_assets = pa.Table.from_pylist(scan_result_to_rows(scan_result))
    row_counts: dict[str, int] = {}
    tables_written: list[str] = []

    image_assets_result = export_table_to_cytomining_warehouse(
        image_assets,
        root,
        table_name=image_assets_table_name,
        role="image_assets",
        join_keys=["dataset_id", "image_id"],
        source_type="scan_result",
        source_ref=scan_result.source_uri,
        mode=mode,
    )
    tables_written.extend(image_assets_result.tables_written)
    row_counts.update(image_assets_result.row_counts)
    manifest_path = image_assets_result.manifest_path

    if include_chunks:
        chunk_index = pa.Table.from_pylist(scan_result_to_chunk_rows(scan_result))
        chunk_result = export_table_to_cytomining_warehouse(
            chunk_index,
            root,
            table_name=chunk_index_table_name,
            role="chunk_index",
            join_keys=["dataset_id", "image_id", "array_path"],
            source_type="scan_result",
            source_ref=scan_result.source_uri,
            mode=mode,
        )
        tables_written.extend(chunk_result.tables_written)
        row_counts.update(chunk_result.row_counts)
        manifest_path = chunk_result.manifest_path

    if profiles is not None:
        joined_profiles = join_profiles_with_scan_result(
            scan_result,
            profiles,
            include_chunks=include_chunks,
            profile_dataset_id=profile_dataset_id,
        )
        joined_result = export_table_to_cytomining_warehouse(
            joined_profiles,
            root,
            table_name=joined_table_name,
            role="joined_profiles",
            join_keys=["dataset_id", "image_id"],
            source_type="joined_profiles",
            source_ref=scan_result.source_uri,
            mode=mode,
        )
        tables_written.extend(joined_result.tables_written)
        row_counts.update(joined_result.row_counts)
        manifest_path = joined_result.manifest_path

    return CytominingWarehouseResult(
        warehouse_root=str(root),
        tables_written=tables_written,
        row_counts=row_counts,
        manifest_path=manifest_path,
    )


def export_store_to_cytomining_warehouse(  # noqa: PLR0913
    uri: str,
    warehouse_root: str | Path,
    *,
    profiles: MetadataSource | None = None,
    include_chunks: bool = True,
    image_assets_table_name: str = DEFAULT_IMAGE_ASSETS_TABLE,
    chunk_index_table_name: str = DEFAULT_CHUNK_INDEX_TABLE,
    joined_table_name: str = DEFAULT_JOINED_PROFILES_TABLE,
    profile_dataset_id: str | None = None,
    mode: WriteMode = "overwrite",
) -> CytominingWarehouseResult:
    """Scan a store and export its metadata into a Cytomining warehouse."""

    from iceberg_bioimage.api import scan_store

    return export_scan_result_to_cytomining_warehouse(
        scan_store(uri),
        warehouse_root,
        profiles=profiles,
        include_chunks=include_chunks,
        image_assets_table_name=image_assets_table_name,
        chunk_index_table_name=chunk_index_table_name,
        joined_table_name=joined_table_name,
        profile_dataset_id=profile_dataset_id,
        mode=mode,
    )


def export_catalog_to_cytomining_warehouse(  # noqa: PLR0913
    catalog: str | SupportsScanCatalog,
    namespace: str | tuple[str, ...],
    warehouse_root: str | Path,
    *,
    profiles: MetadataSource | None = None,
    image_assets_table_name: str = DEFAULT_IMAGE_ASSETS_TABLE,
    chunk_index_table_name: str | None = DEFAULT_CHUNK_INDEX_TABLE,
    joined_table_name: str = DEFAULT_JOINED_PROFILES_TABLE,
    profile_dataset_id: str | None = None,
    mode: WriteMode = "overwrite",
) -> CytominingWarehouseResult:
    """Materialize catalog-backed metadata into a Parquet Cytomining warehouse."""

    from iceberg_bioimage.integrations.catalog import (
        catalog_table_to_arrow,
        join_catalog_image_assets_with_profiles,
    )

    root = Path(warehouse_root)
    row_counts: dict[str, int] = {}
    tables_written: list[str] = []

    image_assets = catalog_table_to_arrow(
        catalog,
        namespace,
        image_assets_table_name,
    )
    image_assets_result = export_table_to_cytomining_warehouse(
        image_assets,
        root,
        table_name=image_assets_table_name,
        role="image_assets",
        join_keys=["dataset_id", "image_id"],
        source_type="catalog",
        source_ref=_catalog_source_ref(catalog, namespace, image_assets_table_name),
        mode=mode,
    )
    tables_written.extend(image_assets_result.tables_written)
    row_counts.update(image_assets_result.row_counts)
    manifest_path = image_assets_result.manifest_path

    if chunk_index_table_name is not None:
        chunk_index = catalog_table_to_arrow(
            catalog,
            namespace,
            chunk_index_table_name,
        )
        chunk_result = export_table_to_cytomining_warehouse(
            chunk_index,
            root,
            table_name=chunk_index_table_name,
            role="chunk_index",
            join_keys=["dataset_id", "image_id", "array_path"],
            source_type="catalog",
            source_ref=_catalog_source_ref(catalog, namespace, chunk_index_table_name),
            mode=mode,
        )
        tables_written.extend(chunk_result.tables_written)
        row_counts.update(chunk_result.row_counts)
        manifest_path = chunk_result.manifest_path

    if profiles is not None:
        joined_profiles = join_catalog_image_assets_with_profiles(
            catalog,
            namespace,
            profiles,
            image_assets_table=image_assets_table_name,
            chunk_index_table=chunk_index_table_name,
            profile_dataset_id=profile_dataset_id,
        )
        joined_result = export_table_to_cytomining_warehouse(
            joined_profiles,
            root,
            table_name=joined_table_name,
            role="joined_profiles",
            join_keys=["dataset_id", "image_id"],
            source_type="catalog_join",
            source_ref=_catalog_source_ref(catalog, namespace, joined_table_name),
            mode=mode,
        )
        tables_written.extend(joined_result.tables_written)
        row_counts.update(joined_result.row_counts)
        manifest_path = joined_result.manifest_path

    return CytominingWarehouseResult(
        warehouse_root=str(root),
        tables_written=tables_written,
        row_counts=row_counts,
        manifest_path=manifest_path,
    )


def export_profiles_to_cytomining_warehouse(  # noqa: PLR0913
    profiles: MetadataSource,
    warehouse_root: str | Path,
    *,
    table_name: str = "profiles",
    role: str = "profiles",
    profile_dataset_id: str | None = None,
    join_keys: list[str] | None = None,
    source_type: str = "profiles",
    source_ref: str | None = None,
    alias_map: Mapping[str, tuple[str, ...] | list[str]] | None = None,
    mode: WriteMode = "append",
) -> CytominingWarehouseResult:
    """Write a Cytomining profile table into a Parquet-backed warehouse root."""

    root = Path(warehouse_root)
    table = _normalize_profiles_table(
        _metadata_source_to_table(profiles),
        profile_dataset_id=profile_dataset_id,
        alias_map=alias_map,
    )
    return export_table_to_cytomining_warehouse(
        table,
        root,
        table_name=table_name,
        role=role,
        join_keys=[] if join_keys is None else join_keys,
        source_type=source_type,
        source_ref=source_ref if source_ref is not None else str(profiles),
        mode=mode,
        default_namespace=DEFAULT_PROFILE_NAMESPACE,
    )


def export_table_to_cytomining_warehouse(  # noqa: PLR0913
    table: pa.Table,
    warehouse_root: str | Path,
    *,
    table_name: str,
    role: str,
    join_keys: list[str] | None = None,
    source_type: str | None = None,
    source_ref: str | None = None,
    mode: WriteMode = "append",
    default_namespace: str | None = None,
) -> CytominingWarehouseResult:
    """Write a generic table into a warehouse root and update the manifest."""

    root = Path(warehouse_root)
    normalized_table_name, dataset_path = _resolve_table_layout(
        root,
        table_name,
        default_namespace=default_namespace,
    )
    _write_parquet_dataset(
        table,
        dataset_path,
        mode=mode,
    )
    manifest_path = _update_manifest(
        root,
        WarehouseTableManifestEntry(
            table_name=normalized_table_name,
            role=role,
            join_keys=[] if join_keys is None else join_keys,
            source_type=source_type,
            source_ref=source_ref,
            row_count=table.num_rows,
            columns=list(table.schema.names),
        ),
    )
    return CytominingWarehouseResult(
        warehouse_root=str(root),
        tables_written=[normalized_table_name],
        row_counts={normalized_table_name: table.num_rows},
        manifest_path=str(manifest_path),
    )


def _resolve_table_layout(
    warehouse_root: Path,
    table_name: str,
    *,
    default_namespace: str | None = None,
) -> tuple[str, Path]:
    normalized = table_name.strip()
    if not normalized:
        raise ValueError("table_name must not be empty.")

    if "." in normalized:
        parts = [part for part in normalized.split(".") if part]
    elif default_namespace is not None:
        parts = [default_namespace, normalized]
    else:
        parts = [normalized]

    normalized_name = ".".join(parts)
    return normalized_name, warehouse_root.joinpath(*parts)


def _write_parquet_dataset(
    table: pa.Table,
    dataset_path: Path,
    *,
    mode: WriteMode,
) -> None:
    if mode not in {"overwrite", "append"}:
        raise ValueError("mode must be either 'overwrite' or 'append'.")

    if mode == "overwrite" and dataset_path.exists():
        shutil.rmtree(dataset_path)

    dataset_path.mkdir(parents=True, exist_ok=True)
    file_path = dataset_path / f"part-{uuid.uuid4().hex}.parquet"
    pq.write_table(table, file_path)


def load_warehouse_manifest(warehouse_root: str | Path) -> WarehouseManifest:
    """Load a warehouse manifest if present, otherwise return an empty manifest."""

    root = Path(warehouse_root)
    manifest_path = root / "warehouse_manifest.json"
    if not manifest_path.exists():
        return WarehouseManifest(warehouse_root=str(root))

    payload = json.loads(manifest_path.read_text())
    return WarehouseManifest(
        warehouse_root=payload["warehouse_root"],
        tables=[
            WarehouseTableManifestEntry(
                table_name=table["table_name"],
                role=table["role"],
                format=table.get("format", "parquet"),
                join_keys=list(table.get("join_keys", [])),
                source_type=table.get("source_type"),
                source_ref=table.get("source_ref"),
                row_count=table.get("row_count"),
                columns=list(table.get("columns", [])),
            )
            for table in payload.get("tables", [])
        ],
    )


def _update_manifest(
    warehouse_root: Path,
    entry: WarehouseTableManifestEntry,
) -> Path:
    manifest = load_warehouse_manifest(warehouse_root)
    manifest.warehouse_root = str(warehouse_root)
    manifest.tables = [
        table for table in manifest.tables if table.table_name != entry.table_name
    ]
    manifest.tables.append(entry)
    manifest_path = warehouse_root / "warehouse_manifest.json"
    warehouse_root.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(manifest.to_json(indent=2, sort_keys=True))
    return manifest_path


def _metadata_source_to_table(source: MetadataSource) -> pa.Table:
    if isinstance(source, pa.Table):
        return source
    if isinstance(source, list):
        return pa.Table.from_pylist(source)
    if isinstance(source, (str, Path)):
        return ds.dataset(source).to_table()

    raise TypeError(f"Unsupported metadata source type: {type(source)!r}")


def _normalize_profiles_table(
    table: pa.Table,
    *,
    profile_dataset_id: str | None,
    alias_map: Mapping[str, tuple[str, ...] | list[str]] | None = None,
) -> pa.Table:
    resolved_columns = resolve_microscopy_profile_columns(
        list(table.schema.names),
        alias_map=alias_map,
    )
    normalized = table

    for canonical in ("dataset_id", "image_id", "plate_id", "well_id", "site_id"):
        if canonical in normalized.schema.names:
            continue

        source = resolved_columns[canonical]
        if source is not None:
            normalized = normalized.append_column(
                canonical,
                normalized[source],
            )
            continue

        if canonical == "dataset_id" and profile_dataset_id is not None:
            normalized = normalized.append_column(
                canonical,
                pa.repeat(pa.scalar(profile_dataset_id), normalized.num_rows),
            )

    return normalized


def _catalog_source_ref(
    catalog: str | SupportsScanCatalog,
    namespace: str | tuple[str, ...],
    table_name: str,
) -> str:
    namespace_label = namespace if isinstance(namespace, str) else ".".join(namespace)
    catalog_label = catalog if isinstance(catalog, str) else type(catalog).__name__
    return f"{catalog_label}:{namespace_label}.{table_name}"
