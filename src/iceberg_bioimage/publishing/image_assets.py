"""Image asset publishing helpers."""

from __future__ import annotations

import json
import warnings
from collections.abc import Iterable
from pathlib import Path
from typing import Callable, Protocol, TypeVar

import pyarrow as pa
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
)

from iceberg_bioimage.models.scan_result import ImageAsset, ScanResult
from iceberg_bioimage.validation.contracts import raise_for_invalid_scan_result

CYTOTABLE_NAMESPACE_SEGMENT = "cytotable"


class SupportsAppend(Protocol):
    """Protocol for appendable Iceberg-like tables."""

    def append(self, table: pa.Table) -> None:
        """Append a pyarrow table."""


TTable = TypeVar("TTable")


class SupportsLoadTable(Protocol[TTable]):
    """Protocol for catalog objects that can load existing tables."""

    def load_table(self, identifier: tuple[str, ...]) -> TTable:
        """Load an existing table."""


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
    requested_namespace = _normalize_namespace(namespace)
    candidate_namespaces = _namespace_candidates(requested_namespace)

    for resolved_namespace in candidate_namespaces:
        identifier = (*resolved_namespace, table_name)
        try:
            table = catalog.load_table(identifier)
        except NoSuchTableError:  # pragma: no cover - depends on active catalog backend
            continue

        _warn_for_namespace_resolution(
            requested_namespace,
            resolved_namespace,
            table_name,
            operation="publishing",
        )
        return table

    build_schema = (
        _build_image_assets_schema if schema_builder is None else schema_builder
    )
    preferred_namespace = candidate_namespaces[0]
    _warn_for_namespace_resolution(
        requested_namespace,
        preferred_namespace,
        table_name,
        operation="publishing",
        creating=True,
    )
    _ensure_namespace_exists(catalog, preferred_namespace)
    identifier = (*preferred_namespace, table_name)
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

    # Field IDs are intentionally non-sequential in _build_image_assets_schema.
    # These stable NestedField identifiers keep compatibility with prior or
    # future schema revisions without reusing removed field IDs.
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

    return tuple(part for part in namespace if part)


def _namespace_candidates(
    namespace: str | Iterable[str],
) -> tuple[tuple[str, ...], ...]:
    normalized_namespace = _normalize_namespace(namespace)
    if _is_cytotable_namespace(normalized_namespace):
        return (normalized_namespace,)

    return ((*normalized_namespace, CYTOTABLE_NAMESPACE_SEGMENT), normalized_namespace)


def _load_table_with_namespace_fallback(
    catalog: SupportsLoadTable[TTable],
    namespace: str | Iterable[str],
    table_name: str,
    *,
    operation: str,
) -> TTable:
    requested_namespace = _normalize_namespace(namespace)

    for resolved_namespace in _namespace_candidates(requested_namespace):
        identifier = (*resolved_namespace, table_name)
        try:
            table = catalog.load_table(identifier)
        except NoSuchTableError:
            continue

        _warn_for_namespace_resolution(
            requested_namespace,
            resolved_namespace,
            table_name,
            operation=operation,
        )
        return table

    identifier = (*_namespace_candidates(requested_namespace)[0], table_name)
    raise NoSuchTableError(f"Missing table: {identifier!r}")


def _list_tables_with_namespace_fallback(
    catalog: SupportsCatalog,
    namespace: str | Iterable[str],
) -> list[tuple[str, ...]]:
    if not hasattr(catalog, "list_tables"):
        raise TypeError("Catalog must provide a list_tables(namespace) method.")

    requested_namespace = _normalize_namespace(namespace)
    discovered: dict[str, tuple[str, ...]] = {}

    for resolved_namespace in _namespace_candidates(requested_namespace):
        try:
            identifiers = catalog.list_tables(resolved_namespace)
        except NoSuchNamespaceError:
            continue

        if identifiers:
            _warn_for_namespace_resolution(
                requested_namespace,
                resolved_namespace,
                "catalog tables",
                operation="listing",
            )
        for identifier in identifiers:
            discovered.setdefault(identifier[-1], identifier)

    return [discovered[table_name] for table_name in sorted(discovered)]


def _ensure_namespace_exists(
    catalog: SupportsCatalog,
    namespace: tuple[str, ...],
) -> None:
    if hasattr(catalog, "create_namespace_if_not_exists"):
        catalog.create_namespace_if_not_exists(namespace)
        return

    if hasattr(catalog, "create_namespace"):
        try:
            catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:  # pragma: no cover - backend-specific
            return


def _is_cytotable_namespace(namespace: tuple[str, ...]) -> bool:
    return bool(namespace) and namespace[-1] == CYTOTABLE_NAMESPACE_SEGMENT


def _warn_for_namespace_resolution(
    requested_namespace: tuple[str, ...],
    resolved_namespace: tuple[str, ...],
    table_name: str,
    *,
    operation: str,
    creating: bool = False,
) -> None:
    if requested_namespace == resolved_namespace and _is_cytotable_namespace(
        requested_namespace
    ):
        return

    expected_namespace_parts = _namespace_candidates(requested_namespace)[0]
    expected_namespace = ".".join(expected_namespace_parts)
    resolved_namespace_name = ".".join(resolved_namespace)
    action = "Creating" if creating else "Using"
    warnings.warn(
        (
            f"Namespace '{'.'.join(requested_namespace)}' does not match "
            f"CytoTable's expected Iceberg namespace layout. {action} "
            f"'{resolved_namespace_name}.{table_name}' during {operation}; "
            f"CytoTable expects '{expected_namespace}.{table_name}'."
        ),
        UserWarning,
        stacklevel=3,
    )


def _dataset_id(uri: str) -> str:
    name = Path(uri.rstrip("/")).name
    for suffix in (".ome.zarr", ".zarr", ".ome.tiff", ".ome.tif", ".tiff", ".tif"):
        if name.endswith(suffix):
            return name.removesuffix(suffix)

    return name


def _fallback_image_id(dataset_id: str, array_path: str | None) -> str:
    return dataset_id if array_path is None else f"{dataset_id}:{array_path}"
