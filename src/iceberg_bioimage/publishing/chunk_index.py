"""Chunk index publishing helpers."""

from __future__ import annotations

import json
from collections.abc import Iterable
from itertools import product
from math import prod

import numpy as np
import pyarrow as pa

from iceberg_bioimage.models.scan_result import ImageAsset, ScanResult
from iceberg_bioimage.publishing.image_assets import (
    SupportsCatalog,
    _dataset_id,
    _fallback_image_id,
    _load_or_create_table,
)
from iceberg_bioimage.validation.contracts import raise_for_invalid_scan_result


def publish_chunk_index(
    catalog: str | SupportsCatalog,
    namespace: str | Iterable[str],
    table_name: str,
    scan_result: ScanResult,
) -> int:
    """Publish derived chunk metadata into the canonical `chunk_index` table."""

    raise_for_invalid_scan_result(scan_result)
    rows = scan_result_to_chunk_rows(scan_result)
    table = _load_or_create_table(
        catalog,
        namespace,
        table_name,
        schema_builder=_build_chunk_index_schema,
    )
    if not rows:
        return 0

    table.append(pa.Table.from_pylist(rows))
    return len(rows)


def scan_result_to_chunk_rows(scan_result: ScanResult) -> list[dict[str, object]]:
    """Convert a scan result into canonical chunk_index rows."""

    dataset_id = _dataset_id(scan_result.source_uri)
    rows: list[dict[str, object]] = []

    for asset in scan_result.image_assets:
        rows.extend(_asset_to_chunk_rows(dataset_id, asset))

    return rows


def _asset_to_chunk_rows(
    dataset_id: str,
    asset: ImageAsset,
) -> list[dict[str, object]]:
    if asset.chunk_shape is None:
        return []

    dtype = np.dtype(asset.dtype)
    shape = list(asset.shape)
    chunk_shape = list(asset.chunk_shape)
    image_id = asset.image_id or _fallback_image_id(dataset_id, asset.array_path)
    chunk_grid = [
        range((axis_size + chunk_size - 1) // chunk_size)
        for axis_size, chunk_size in zip(shape, chunk_shape, strict=True)
    ]

    rows: list[dict[str, object]] = []
    for chunk_coords in product(*chunk_grid):
        start_coords = [
            coord * chunk_size
            for coord, chunk_size in zip(chunk_coords, chunk_shape, strict=True)
        ]
        actual_chunk_shape = [
            min(chunk_size, axis_size - start_coord)
            for axis_size, chunk_size, start_coord in zip(
                shape,
                chunk_shape,
                start_coords,
                strict=True,
            )
        ]
        rows.append(
            {
                "dataset_id": dataset_id,
                "image_id": image_id,
                "array_path": asset.array_path,
                "chunk_key": _chunk_key(chunk_coords),
                "chunk_coords_json": json.dumps(list(chunk_coords)),
                "byte_length": int(prod(actual_chunk_shape) * dtype.itemsize),
            }
        )

    return rows


def _chunk_key(chunk_coords: tuple[int, ...]) -> str:
    return "/".join(str(coord) for coord in chunk_coords)


def _build_chunk_index_schema() -> object:
    try:
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField, StringType
    except ImportError as exc:  # pragma: no cover - guarded by dependency declaration
        raise RuntimeError(
            "PyIceberg is required for publishing. Install `pyiceberg` first."
        ) from exc

    return Schema(
        NestedField(
            field_id=101,
            name="dataset_id",
            field_type=StringType(),
            required=True,
        ),
        NestedField(
            field_id=102,
            name="image_id",
            field_type=StringType(),
            required=True,
        ),
        NestedField(
            field_id=103,
            name="array_path",
            field_type=StringType(),
            required=False,
        ),
        NestedField(
            field_id=104,
            name="chunk_key",
            field_type=StringType(),
            required=True,
        ),
        NestedField(
            field_id=105,
            name="chunk_coords_json",
            field_type=StringType(),
            required=True,
        ),
        NestedField(
            field_id=106,
            name="byte_length",
            field_type=LongType(),
            required=False,
        ),
    )
