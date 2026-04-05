# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.17.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Warehouse Namespace Demo
#
# This notebook shows a minimal end-to-end warehouse workflow for the
# `cytomining` ecosystem:
#
# - build a tiny Zarr store and a tiny OME-TIFF store
# - ingest both through the warehouse API
# - show the preferred Cytotable-compatible namespace layout
# - show how legacy namespaces are still supported
# - show how `pycytominer` and `coSMicQC`-style `Metadata_*` columns join to
#   the warehouse without manual renaming
#

# +
from __future__ import annotations

import tempfile
import warnings
from pathlib import Path

import numpy as np
import pyarrow as pa
import tifffile
import zarr
from pyiceberg.exceptions import NoSuchTableError

from iceberg_bioimage import (
    ingest_stores_to_warehouse,
    join_profiles_with_store,
    scan_store,
    summarize_store,
)


class DemoTable:
    def __init__(self) -> None:
        self.appends: list[pa.Table] = []

    def append(self, table: pa.Table) -> None:
        self.appends.append(table)


class DemoCatalog:
    def __init__(self, tables: dict[tuple[str, ...], DemoTable] | None = None) -> None:
        self.tables = {} if tables is None else dict(tables)
        self.created_namespaces: list[tuple[str, ...]] = []
        self.created_identifiers: list[tuple[str, ...]] = []

    def load_table(self, identifier: tuple[str, ...]) -> DemoTable:
        if identifier not in self.tables:
            raise NoSuchTableError(f"Missing table: {identifier!r}")
        return self.tables[identifier]

    def create_table(self, identifier: tuple[str, ...], schema: object) -> DemoTable:
        if identifier in self.tables:
            raise ValueError(f"Table already exists: {identifier!r}")
        self.created_identifiers.append(identifier)
        table = DemoTable()
        self.tables[identifier] = table
        return table

    def create_namespace_if_not_exists(self, namespace: tuple[str, ...]) -> None:
        self.created_namespaces.append(namespace)

    def list_tables(self, namespace: tuple[str, ...]) -> list[tuple[str, ...]]:
        return [
            identifier for identifier in self.tables if identifier[:-1] == namespace
        ]


def warehouse_snapshot(catalog: DemoCatalog) -> dict[str, list[dict[str, object]]]:
    snapshot: dict[str, list[dict[str, object]]] = {}
    for identifier, table in sorted(catalog.tables.items()):
        rows: list[dict[str, object]] = []
        for appended in table.appends:
            rows.extend(appended.to_pylist())
        snapshot[".".join(identifier)] = rows
    return snapshot


# -

with tempfile.TemporaryDirectory(prefix="iceberg-bioimage-demo-") as tmpdir_ctx:
    tmpdir = Path(tmpdir_ctx)

    zarr_path = tmpdir / "plate.zarr"
    root = zarr.open_group(zarr_path, mode="w", zarr_version=2)
    root.attrs["multiscales"] = [{"axes": ["c", "y", "x"], "datasets": [{"path": "0"}]}]
    root.create_dataset(
        "0",
        shape=(1, 4, 4),
        data=np.arange(16, dtype=np.uint16).reshape(1, 4, 4),
        chunks=(1, 2, 2),
    )

    tiff_path = tmpdir / "cells.ome.tiff"
    tifffile.imwrite(tiff_path, np.arange(24, dtype=np.uint8).reshape(2, 3, 4))

    zarr_summary = summarize_store(str(zarr_path)).to_dict()
    tiff_summary = summarize_store(str(tiff_path)).to_dict()

    {
        "zarr_summary": zarr_summary,
        "tiff_summary": tiff_summary,
    }
    # -

    # ## Preferred Cytotable namespace
    #
    # When you ingest into namespace `bioimage`, this project prefers the
    # Cytotable-compatible layout `bioimage.cytotable.*` for new warehouse tables.
    # The two canonical tables are:
    #
    # - `image_assets`: one row per discovered image asset
    # - `chunk_index`: one row per chunk when chunk metadata is available
    #

    # +
    catalog = DemoCatalog()
    warehouse = ingest_stores_to_warehouse(
        [str(zarr_path), str(tiff_path)],
        catalog,
        "bioimage",
    )

    {
        "warehouse_result": warehouse.to_dict(),
        "created_namespaces": catalog.created_namespaces,
        "created_identifiers": catalog.created_identifiers,
        "warehouse_snapshot": warehouse_snapshot(catalog),
    }
    # -

    # ## Legacy namespace fallback
    #
    # Existing warehouses may already store tables directly under `bioimage.*`.
    # When those legacy tables already exist, the ingest path reuses them instead of
    # creating a second copy under `bioimage.cytotable.*`, and it emits a warning so
    # the layout difference is visible.
    #

    # +
    legacy_catalog = DemoCatalog(
        tables={
            ("bioimage", "image_assets"): DemoTable(),
            ("bioimage", "chunk_index"): DemoTable(),
        }
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        legacy_result = ingest_stores_to_warehouse(
            [str(zarr_path)],
            legacy_catalog,
            "bioimage",
        )

    {
        "legacy_result": legacy_result.to_dict(),
        "legacy_identifiers": sorted(
            ".".join(identifier) for identifier in legacy_catalog.tables
        ),
        "warnings": [str(item.message) for item in caught],
    }
    # -

    # ## Cytomining profile tables
    #
    # `pycytominer` and `coSMicQC` profile tables often use `Metadata_*` columns.
    # This project now normalizes common aliases like:
    #
    # - `Metadata_dataset_id -> dataset_id`
    # - `Metadata_ImageID -> image_id`
    # - `Metadata_Plate -> plate_id`
    # - `Metadata_Well -> well_id`
    # - `Metadata_Site -> site_id`
    #
    # That means profile tables from the Cytomining ecosystem can join to the
    # warehouse without manual renaming when those aliases are present.
    #

    # +
    zarr_scan = scan_store(str(zarr_path))
    zarr_asset = zarr_scan.image_assets[0]

    profiles = pa.table(
        {
            "Metadata_dataset_id": ["plate"],
            "Metadata_ImageID": [zarr_asset.image_id],
            "Metadata_Plate": ["Plate-1"],
            "Metadata_Well": ["A01"],
            "Metadata_Site": ["1"],
            "cell_count": [42],
        }
    )

    joined = join_profiles_with_store(str(zarr_path), profiles)
    joined.select(
        ["dataset_id", "image_id", "plate_id", "well_id", "site_id", "cell_count"]
    ).to_pydict()
