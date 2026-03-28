"""Image asset publishing helpers."""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Callable, Protocol

import pyarrow as pa

from iceberg_bioimage.models.scan_result import ImageAsset, ScanResult
from iceberg_bioimage.validation.contracts import raise_for_invalid_scan_result


class SupportsAppend(Protocol):
    """Protocol for appendable Iceberg-like tables."""

    def append(self, table: pa.Table) -> None:
        """Append a pyarrow table."""


class SupportsCatalog(Protocol):
    """Protocol for catalog objects used by the publishing layer."""

    def load_table(self, identifier: tuple[str, ...]) -> SupportsAppend:
        """Load an existing table."""

    def create_table(
        self,
        identifier: tuple[str, ...],
        schema: object,
    ) -> SupportsAppend:
        """Create and return a table."""


def publish_image_assets(
    catalog: str | SupportsCatalog,
    namespace: str | Iterable[str],
    table_name: str,
    scan_result: ScanResult,
) -> int:
    """Publish a scan result into the canonical `image_assets` Iceberg table."""

    raise_for_invalid_scan_result(scan_result)
    rows = scan_result_to_rows(scan_result)
    table = _load_or_create_table(catalog, namespace, table_name)
    table.append(pa.Table.from_pylist(rows))
    return len(rows)


def scan_result_to_rows(scan_result: ScanResult) -> list[dict[str, object]]:
    """Convert a scan result into canonical image_assets rows."""

    dataset_id = _dataset_id(scan_result.source_uri)
    return [
        _asset_to_row(
            dataset_id=dataset_id,
            format_family=scan_result.format_family,
            asset=asset,
        )
        for asset in scan_result.image_assets
    ]


def _asset_to_row(
    dataset_id: str,
    format_family: str,
    asset: ImageAsset,
) -> dict[str, object]:
    return {
        "dataset_id": dataset_id,
        "image_id": asset.image_id or _fallback_image_id(dataset_id, asset.array_path),
        "format_family": format_family,
        "uri": asset.uri,
        "array_path": asset.array_path,
        "shape_json": json.dumps(asset.shape),
        "dtype": asset.dtype,
        "chunk_shape_json": (
            json.dumps(asset.chunk_shape) if asset.chunk_shape else None
        ),
        "metadata_json": json.dumps(asset.metadata, sort_keys=True)
        if asset.metadata
        else None,
    }


def _load_or_create_table(
    catalog_or_name: str | SupportsCatalog,
    namespace: str | Iterable[str],
    table_name: str,
    *,
    schema_builder: Callable[[], object] | None = None,
) -> SupportsAppend:
    catalog = _resolve_catalog(catalog_or_name)
    identifier = (*_normalize_namespace(namespace), table_name)

    try:
        return catalog.load_table(identifier)
    except Exception as exc:  # pragma: no cover - depends on active catalog backend
        if getattr(exc.__class__, "__name__", "") != "NoSuchTableError":
            raise

    build_schema = (
        _build_image_assets_schema if schema_builder is None else schema_builder
    )
    return catalog.create_table(identifier, schema=build_schema())


def _resolve_catalog(catalog_or_name: str | SupportsCatalog) -> SupportsCatalog:
    if not isinstance(catalog_or_name, str):
        return catalog_or_name

    try:
        from pyiceberg.catalog import load_catalog
    except ImportError as exc:  # pragma: no cover - guarded by dependency declaration
        raise RuntimeError(
            "PyIceberg is required to resolve a catalog by name. "
            "Install `pyiceberg` first."
        ) from exc

    return load_catalog(catalog_or_name)


def _build_image_assets_schema() -> object:
    try:
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, StringType
    except ImportError as exc:  # pragma: no cover - guarded by dependency declaration
        raise RuntimeError(
            "PyIceberg is required for publishing. Install `pyiceberg` first."
        ) from exc

    return Schema(
        NestedField(
            field_id=1,
            name="dataset_id",
            field_type=StringType(),
            required=True,
        ),
        NestedField(
            field_id=2,
            name="image_id",
            field_type=StringType(),
            required=True,
        ),
        NestedField(
            field_id=9,
            name="format_family",
            field_type=StringType(),
            required=True,
        ),
        NestedField(
            field_id=11,
            name="uri",
            field_type=StringType(),
            required=True,
        ),
        NestedField(
            field_id=13,
            name="array_path",
            field_type=StringType(),
            required=False,
        ),
        NestedField(
            field_id=14,
            name="shape_json",
            field_type=StringType(),
            required=True,
        ),
        NestedField(
            field_id=15,
            name="chunk_shape_json",
            field_type=StringType(),
            required=False,
        ),
        NestedField(
            field_id=16,
            name="dtype",
            field_type=StringType(),
            required=True,
        ),
        NestedField(
            field_id=19,
            name="metadata_json",
            field_type=StringType(),
            required=False,
        ),
    )


def _normalize_namespace(namespace: str | Iterable[str]) -> tuple[str, ...]:
    if isinstance(namespace, str):
        return tuple(part for part in namespace.split(".") if part)

    return tuple(namespace)


def _dataset_id(uri: str) -> str:
    name = Path(uri.rstrip("/")).name
    for suffix in (".ome.zarr", ".zarr", ".ome.tiff", ".ome.tif", ".tiff", ".tif"):
        if name.endswith(suffix):
            return name.removesuffix(suffix)

    return name


def _fallback_image_id(dataset_id: str, array_path: str | None) -> str:
    return dataset_id if array_path is None else f"{dataset_id}:{array_path}"
