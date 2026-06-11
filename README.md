<img src="https://raw.githubusercontent.com/wayscience/iceberg-bioimage/main/docs/src/_static/iceberg-bioimage-logo.png" alt="iceberg-bioimage logo" width="150">

# iceberg-bioimage

[![Software DOI badge](https://zenodo.org/badge/DOI/10.5281/zenodo.19672521.svg)](https://doi.org/10.5281/zenodo.19672521)
[![PyPI - Version](https://img.shields.io/pypi/v/iceberg-bioimage)](https://pypi.org/project/iceberg-bioimage/)
[![Build Status](https://github.com/wayscience/iceberg-bioimage/actions/workflows/run-tests.yml/badge.svg?branch=main)](https://github.com/wayscience/iceberg-bioimage/actions/workflows/run-tests.yml?query=branch%3Amain)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

`iceberg-bioimage` scans OME-TIFF and OME-Zarr images into queryable Arrow tables and optionally catalogs them with Apache Iceberg.

## Install

```bash
pip install iceberg-bioimage
```

Optional extras:

```bash
pip install 'iceberg-bioimage[duckdb]'     # DuckDB query helpers
pip install 'iceberg-bioimage[ome-arrow]'  # OME-Arrow image payloads (≥0.0.10)
pip install 'iceberg-bioimage[s3]'         # S3 / remote URI support
```

______________________________________________________________________

## Usage

### 1 — Virtual datasets (no catalog required)

Scan any image directly into an Arrow table. No Iceberg catalog, no setup.

```python
from iceberg_bioimage import scan_as_arrow_table

table = scan_as_arrow_table("data/experiment.ome.tiff")
# or: scan_as_arrow_table("data/experiment.zarr")
# or: scan_as_arrow_table("s3://bucket/experiment.ome.tiff")

print(table.schema)
# dataset_id, image_id, uri, format_family, shape_json, dtype, ...
```

The result is a standard `pyarrow.Table` — query it with DuckDB, Pandas, or pass it directly to downstream tools.

```python
import duckdb

duckdb.query("SELECT dataset_id, shape_json FROM table WHERE dtype = 'uint16'")
```

### 2 — Catalog registration

Register images into an Apache Iceberg catalog so they can be queried across experiments.

```python
from iceberg_bioimage import register_store

register_store("data/experiment.zarr", "default", "myproject.images")
```

Register a whole directory at once:

```python
from iceberg_bioimage import register_directory

register_directory(
    "data/plates/",
    "default",
    "myproject.images",
    glob="**/*.ome.tiff",   # default is **/*.ome.zarr
)
```

Replace an existing registration (upsert):

```python
register_store("data/experiment.zarr", "default", "myproject.images", replace=True)
```

Remove a dataset from the catalog:

```python
from iceberg_bioimage import deregister_store

deregister_store("data/experiment.zarr", "default", "myproject.images")
```

Skip chunk indexing (recommended for TIFF-only datasets):

```python
register_store("data/experiment.ome.tiff", "default", "myproject.images", chunk_index_table=None)
```

### 3 — Profile tables

Publish a CellProfiler / pycytominer profile Parquet into the catalog under a `profiles` namespace. Column aliases for common pycytominer conventions (`Image_Metadata_Well`, `Metadata_Plate`, etc.) are resolved automatically.

```python
from iceberg_bioimage import register_profile_table

register_profile_table(
    "data/profiles.parquet",
    "default",
    "myproject.profiles",   # conventional: <experiment>.profiles
)
```

Validate a profile table against the microscopy join contract before registering:

```python
from iceberg_bioimage import validate_microscopy_profile_table

result = validate_microscopy_profile_table("data/profiles.parquet")
print(result.is_valid)
print(result.missing_required_columns)   # ['dataset_id', 'image_id'] if unresolvable
print(result.warnings)                   # alias normalization notes
```

### 4 — Joining images to profiles

```python
from iceberg_bioimage import join_profiles_with_store

joined = join_profiles_with_store("data/experiment.zarr", "data/profiles.parquet")
print(joined.num_rows)
```

When `dataset_id` is absent from the profile table but all rows belong to one dataset:

```python
joined = join_profiles_with_store(
    "data/experiment.zarr",
    "data/profiles.parquet",
    profile_dataset_id="experiment",
)
```

### 5 — Cytomining warehouse export

Export images and profiles into a Parquet warehouse layout that `pycytominer` and CytoTable can consume directly:

```python
from iceberg_bioimage import export_store_to_cytomining_warehouse

export_store_to_cytomining_warehouse(
    "data/experiment.zarr",
    "warehouse-root",
    profiles="data/profiles.parquet",
    profile_dataset_id="experiment",
)
```

This writes:

```
warehouse-root/
  images/image_assets/
  profiles/joined_profiles/
```

### 6 — CLI

```bash
iceberg-bioimage scan data/experiment.zarr
iceberg-bioimage summarize data/experiment.zarr
iceberg-bioimage register --catalog default --namespace myproject.images data/experiment.zarr
iceberg-bioimage ingest --catalog default --namespace myproject.images data/a.zarr data/b.zarr
iceberg-bioimage export-cytomining --warehouse-root warehouse-root data/experiment.zarr
iceberg-bioimage validate-contract data/profiles.parquet
iceberg-bioimage join-profiles data/experiment.zarr data/profiles.parquet --output joined.parquet
```

______________________________________________________________________

## S3 and remote URIs

Zarr stores on S3 work with no extra configuration (via `fsspec`). OME-TIFF on S3 requires the `s3` extra:

```bash
pip install 'iceberg-bioimage[s3]'
```

```python
table = scan_as_arrow_table("s3://my-bucket/plates/experiment.ome.tiff")
register_directory("s3://my-bucket/plates/", catalog, "myproject.images")
```

______________________________________________________________________

## Namespace layout

A typical experiment in the catalog looks like:

```
myproject.images   → image_assets table  (one row per image file)
                   → chunk_index table   (optional; Zarr chunked arrays only)
myproject.profiles → profiles table      (pycytominer measurements)
```

______________________________________________________________________

## OME-Arrow integration

For Arrow-native tabular image payloads (requires `ome-arrow >= 0.0.10`):

```python
from iceberg_bioimage import (
    create_ome_arrow_from_tiff,
    create_ome_arrow_from_zarr,
    open_ome_arrow_dataset,
    write_ome_arrow_dataset,
)
```

Install with `pip install 'iceberg-bioimage[ome-arrow]'`.

______________________________________________________________________

## DuckDB helpers

```python
from iceberg_bioimage import join_image_assets_with_profiles, query_metadata_table

joined = join_image_assets_with_profiles(image_assets_table, profiles_table)
filtered = query_metadata_table(joined, filters=[("cell_count", ">", 10)])
```

Install with `pip install 'iceberg-bioimage[duckdb]'`.

______________________________________________________________________

## Troubleshooting

| Problem                                                 | Fix                                                                                                              |
| ------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `DuckDB helpers require the optional duckdb dependency` | `pip install 'iceberg-bioimage[duckdb]'`                                                                         |
| `fsspec is required to open remote TIFF URIs`           | `pip install 'iceberg-bioimage[s3]'`                                                                             |
| Profile fails join contract (`missing dataset_id`)      | Pass `profile_dataset_id=` to join helpers, or check `validate_microscopy_profile_table()` for alias suggestions |
| `Missing table: ...` for catalog-backed paths           | Check catalog config, namespace spelling, and table names                                                        |

______________________________________________________________________

## Documentation

- `docs/src/getting-started.md` — first-time setup
- `docs/src/catalog-setup.md` — catalog configuration
- `docs/src/cytomining.md` — warehouse export workflows
- `docs/src/warehouse-spec.md` — warehouse interoperability specification
- `examples/quickstart.py` — minimal scan, publish, validation
- `examples/catalog_duckdb.py` — catalog-backed query workflow
- `examples/synthetic_workflow.py` — catalog-free local workflow
