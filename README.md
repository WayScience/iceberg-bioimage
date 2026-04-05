# iceberg-bioimage

`iceberg-bioimage` is a format-agnostic Python package for cataloging bioimaging data with Apache Iceberg and exporting Cytomining-compatible warehouse layouts.

Core idea:

- Iceberg is the control plane for cataloging, schemas, joins, and snapshots.
- Cytomining-compatible Parquet warehouses are a first-class export target.
- Zarr and OME-TIFF remain the data plane.
- Adapters normalize each format into a pure-Python `ScanResult`.
- Execution stays in external tools such as DuckDB, xarray, and tifffile.

## Package layout

```text
src/iceberg_bioimage/
  __init__.py
  api.py
  cli.py
  adapters/
  integrations/
  models/
  publishing/
  validation/
```

## Minimal dependencies

- `pyarrow`
- `pyiceberg`
- `tifffile`
- `zarr`

Optional integration groups:

- `duckdb` for query helpers and examples
- `ome-arrow` for Arrow-native tabular image payloads and lazy image access

## Zarr support

`iceberg-bioimage` keeps the user-facing API simple: use `scan_store(...)` for
both local Zarr v2 stores and local Zarr v3 metadata stores.

- Zarr v2 arrays are scanned through the `zarr` Python package
- Local Zarr v3 stores are scanned from `zarr.json` metadata without requiring
  a separate API
- Summaries report the storage variant as `zarr-v2` or `zarr-v3`
- The base package now allows either Zarr 2 or Zarr 3 runtimes so optional
  forward-facing integrations can coexist in the same environment

## Quickstart

```python
from iceberg_bioimage import (
    export_store_to_cytomining_warehouse,
    ingest_stores_to_warehouse,
    join_profiles_with_store,
    register_store,
    summarize_store,
    validate_microscopy_profile_table,
)

registration = register_store("data/experiment.zarr", "default", "bioimage")
print(registration.to_dict())

summary = summarize_store("data/experiment.zarr")
print(summary.to_dict())

contract = validate_microscopy_profile_table("data/cells.parquet")
print(contract.is_valid)

# Requires the optional DuckDB integration:
#   pip install 'iceberg-bioimage[duckdb]'
joined = join_profiles_with_store("data/experiment.zarr", "data/cells.parquet")
print(joined.num_rows)

warehouse = ingest_stores_to_warehouse(
    ["data/experiment-a.zarr", "data/experiment-b.zarr"],
    "default",
    "bioimage",
)
print(warehouse.to_dict())

cytomining_export = export_store_to_cytomining_warehouse(
    "data/experiment-a.zarr",
    "warehouse-root",
    profiles="data/cells.parquet",
    profile_dataset_id="experiment-a",
)
print(cytomining_export.to_dict())
```

```bash
iceberg-bioimage scan data/experiment.zarr
iceberg-bioimage summarize data/experiment.zarr
iceberg-bioimage register --catalog default --namespace bioimage data/experiment.zarr
iceberg-bioimage ingest --catalog default --namespace bioimage data/experiment-a.zarr data/experiment-b.zarr
iceberg-bioimage export-cytomining --warehouse-root warehouse-root data/experiment.zarr
iceberg-bioimage publish-chunks --catalog default --namespace bioimage data/experiment.zarr
iceberg-bioimage register --catalog default --namespace bioimage --publish-chunks data/experiment.zarr
iceberg-bioimage validate-contract data/cells.parquet
iceberg-bioimage join-profiles data/experiment.zarr data/cells.parquet --output joined.parquet
```

- `examples/quickstart.py` for a minimal scan, publish, and validation script
- `examples/catalog_duckdb.py` for a catalog-backed query workflow
- `examples/synthetic_workflow.py` for a self-contained local workflow

Install optional integrations with:

```bash
pip install 'iceberg-bioimage[duckdb]'
pip install 'iceberg-bioimage[ome-arrow]'
```

## DuckDB helpers

DuckDB is supported as an optional integration layer, not as a required engine.
The join helpers also accept common `pycytominer` and `coSMicQC`-style
`Metadata_*` aliases for `dataset_id`, `image_id`, `plate_id`, `well_id`, and
`site_id`. If a profile table is missing `dataset_id` but all rows belong to
one dataset, pass `profile_dataset_id=...` to the high-level join helpers.

```python
import pyarrow as pa

from iceberg_bioimage import join_image_assets_with_profiles, query_metadata_table

image_assets = pa.table(
    {
        "dataset_id": ["ds-1"],
        "image_id": ["img-1"],
        "array_path": ["0"],
        "uri": ["data/example.zarr"],
    }
)
profiles = pa.table(
    {
        "dataset_id": ["ds-1"],
        "image_id": ["img-1"],
        "cell_count": [42],
    }
)

joined = join_image_assets_with_profiles(image_assets, profiles)
filtered = query_metadata_table(
    joined,
    filters=[("cell_count", ">", 10)],
)
```

Install the optional integration with `uv sync --group duckdb`.

## Cytomining warehouse export

This project now treats Cytomining interoperability as a primary workflow.
Besides publishing canonical metadata to Iceberg, it can materialize a
Parquet-backed warehouse root that tools like `pycytominer` can consume
directly.

```python
from iceberg_bioimage import export_store_to_cytomining_warehouse

result = export_store_to_cytomining_warehouse(
    "data/experiment.zarr",
    "warehouse-root",
    profiles="data/profiles.parquet",
    profile_dataset_id="experiment",
)
print(result.to_dict())
```

This writes one or more of:

- `image_assets/`
- `chunk_index/`
- `joined_profiles/`

It can also append downstream Cytomining tables into the same warehouse root,
for example:

- `pycytominer_profiles/`
- `cosmicqc_profiles/`

## OME-Arrow helpers

OME-Arrow is available as an optional forward-facing integration for tabular
image payloads stored in Arrow-compatible formats.

```python
from iceberg_bioimage import create_ome_arrow, scan_ome_arrow

oa = create_ome_arrow("image.ome.tiff")
lazy_oa = scan_ome_arrow("image.ome.parquet")
```

Install it with `uv sync --group ome-arrow` or
`pip install 'iceberg-bioimage[ome-arrow]'`.

## Local synthetic workflow

For a catalog-free onboarding path, `examples/synthetic_workflow.py` creates a
small Zarr store and profile table, validates the join contract, derives
canonical metadata rows, and joins them with the optional DuckDB helpers.

Run it with:

```bash
uv run --group duckdb python examples/synthetic_workflow.py
```

## Catalog-backed query workflow

If you already published canonical metadata tables, you can read them from a
catalog and join them to analysis outputs directly:

```python
import pyarrow as pa

from iceberg_bioimage import join_catalog_image_assets_with_profiles

profiles = pa.table(
    {
        "dataset_id": ["ds-1"],
        "image_id": ["img-1"],
        "cell_count": [42],
    }
)

joined = join_catalog_image_assets_with_profiles(
    "default",
    "bioimage",
    profiles,
    chunk_index_table="chunk_index",
)
```

## Current scope

- Scan Zarr and OME-TIFF stores into canonical `ScanResult` objects
- Summarize scanned datasets into user-facing `DatasetSummary` objects
- Publish `image_assets` and `chunk_index` metadata tables with PyIceberg
- Ingest one or more existing datasets into Cytotable-compatible Iceberg warehouses
- Export new or existing datasets into Cytomining-compatible Parquet warehouses
- Validate profile tables against the microscopy join contract
- Join scanned image metadata to profile tables through a simple top-level API
- Query canonical metadata through optional DuckDB helpers
- Load catalog-backed metadata tables into Arrow for downstream joins

## Design note

After reviewing the current `CytoTable`, `pycytominer`, and `ome-arrow`
codebases, this package now treats Cytomining-compatible warehouse generation
as one of its main jobs. The core package focuses on metadata scanning,
publishing, Cytomining warehouse export, validation, and joins; OME-Arrow
remains the right place for Arrow-native image payload handling and lazy image
access.
