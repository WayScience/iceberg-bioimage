<img src="https://raw.githubusercontent.com/wayscience/iceberg-bioimage/main/docs/src/_static/iceberg-bioimage-logo.png" alt="iceberg-bioimage logo" width="150">

# iceberg-bioimage

[![Software DOI badge](https://zenodo.org/badge/DOI/10.5281/zenodo.19672521.svg)](https://doi.org/10.5281/zenodo.19672521)
[![PyPI - Version](https://img.shields.io/pypi/v/iceberg-bioimage)](https://pypi.org/project/iceberg-bioimage/)
[![Build Status](https://github.com/wayscience/iceberg-bioimage/actions/workflows/run-tests.yml/badge.svg?branch=main)](https://github.com/wayscience/iceberg-bioimage/actions/workflows/run-tests.yml?query=branch%3Amain)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

`iceberg-bioimage` reads the metadata of OME-TIFF and OME-Zarr image files
(shape, dtype, axes, chunking — not pixel data) into Arrow tables that can be
queried directly or cataloged in Apache Iceberg.

## Two terms used throughout this README

- **scan** — read an image's metadata (shape, dtype, axes, chunk layout) without
  loading pixel data. Fast and constant-time regardless of image size.
- **store** — a single image dataset this package can scan: one Zarr directory,
  or one OME-TIFF file, on local disk or remote (S3) storage.

> **Note on "registration":** in this README, "register"/"registration" means
> *recording an image's metadata as a row in an Iceberg table*, not the
> bioimage-analysis sense of spatially aligning two images.

## Install

```bash
pip install iceberg-bioimage
```

Optional extras:

```bash
pip install 'iceberg-bioimage[duckdb]'     # DuckDB query helpers
pip install 'iceberg-bioimage[ome-arrow]'  # OME-Arrow image pixel-data access (≥0.0.10)
pip install 'iceberg-bioimage[s3]'         # S3 / remote URI support for OME-TIFF
```

______________________________________________________________________

## Usage

### 1 — Virtual datasets (no catalog required)

Scan any image directly into an Arrow table.
No Iceberg catalog, no setup.
This is the fastest path to start exploring a dataset: only the file's
metadata is read, so it works the same whether the image is 1 MB or 100 GB.

```python
from iceberg_bioimage import scan_as_arrow_table

table = scan_as_arrow_table("data/experiment.ome.tiff")
# or: scan_as_arrow_table("data/experiment.zarr")
# or: scan_as_arrow_table("s3://bucket/experiment.ome.tiff")

print(table.schema)
# dataset_id, image_id, uri, format_family, shape_json, dtype, ...
```

The result is a standard `pyarrow.Table` — query it with DuckDB, Pandas, or
pass it directly to downstream tools.

```python
import duckdb

duckdb.query("SELECT dataset_id, shape_json FROM table WHERE dtype = 'uint16'")
```

### 2 — Cataloging images in Iceberg

Once you have many datasets and want to query across them, write their
metadata into a persistent Apache Iceberg catalog instead of re-scanning files
each time.

```python
from iceberg_bioimage import register_store

register_store("data/experiment.zarr", "default", "myproject.images")
```

Register a whole directory of stores at once:

```python
from iceberg_bioimage import register_directory

register_directory(
    "data/plates/",
    "default",
    "myproject.images",
    glob="**/*.ome.tiff",   # default is **/*.ome.zarr
)
```

Use `replace=True` to re-register a store that already exists in the catalog —
for example, after re-running a pipeline that regenerated the source image and
you want the catalog row updated instead of duplicated:

```python
register_store("data/experiment.zarr", "default", "myproject.images", replace=True)
```

Remove a dataset's rows from the catalog entirely:

```python
from iceberg_bioimage import deregister_store

deregister_store("data/experiment.zarr", "default", "myproject.images")
```

Pass `chunk_index_table=None` to skip writing the optional chunk-index table.
Recommended for TIFF-only datasets, since TIFF isn't stored as Zarr-style
chunks — the table would always end up empty for TIFF data, so skipping it
avoids an unused table and unnecessary writes:

```python
register_store("data/experiment.ome.tiff", "default", "myproject.images", chunk_index_table=None)
```

→ details: [`docs/src/catalog-setup.md`](docs/src/catalog-setup.md)

### 3 — Profile tables

A "profile table" here means a CellProfiler / Pycytominer measurements file
(cell counts, intensities, shape features, etc.), keyed by image and/or well.
Publish one into the catalog under a `profiles` namespace:

```python
from iceberg_bioimage import register_profile_table

register_profile_table(
    "data/profiles.parquet",
    "default",
    "myproject.profiles",   # conventional: <experiment>.profiles
)
```

Pycytominer tools name their metadata columns inconsistently across pipelines
(`Image_Metadata_Well`, `Metadata_Plate`, `Image_Metadata_Well_x`, ...).
"Alias resolution" means this package recognizes those common naming variants
and maps each one to a canonical column name (`well_id`, `plate_id`, etc.) so
joins work without you renaming columns by hand.

Before registering, you can check whether a profile table has everything
needed to join against image metadata. This package's **microscopy join
contract** is the set of canonical columns a join needs: `dataset_id` and
`image_id` are required, `plate_id`/`well_id`/`site_id` are recommended but
optional.

```python
from iceberg_bioimage import validate_microscopy_profile_table

result = validate_microscopy_profile_table("data/profiles.parquet")

print(result.is_valid)
# False if dataset_id/image_id can't be found or aliased

print(result.missing_required_columns)
# e.g. ['dataset_id', 'image_id'] — columns the join contract needs but
# this table doesn't have, even after alias matching

print(result.warnings)
# e.g. ["well_id resolved from Image_Metadata_Well"] — non-standard column
# names that were matched to canonical join keys
```

### 4 — Joining images to profiles

```python
from iceberg_bioimage import join_profiles_with_store

joined = join_profiles_with_store("data/experiment.zarr", "data/profiles.parquet")
print(joined.num_rows)
```

When `dataset_id` is absent from the profile table but all rows belong to one
dataset, supply it directly instead of relying on alias resolution:

```python
joined = join_profiles_with_store(
    "data/experiment.zarr",
    "data/profiles.parquet",
    profile_dataset_id="experiment",
)
```

### 5 — Cytomining warehouse export

Export images and profiles into a Parquet warehouse layout that `pycytominer`
and CytoTable can consume directly, without going through Iceberg at all:

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

→ details: [`docs/src/cytomining.md`](docs/src/cytomining.md)

### 6 — CLI

A command-line wrapper around the same functions, for use in shell scripts and
CI pipelines where writing a Python file isn't worth it. Each subcommand
mirrors a Python function from the sections above:

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

Zarr stores on S3 work out of the box — no extra install needed — because `zarr` already pulls in `fsspec`.
OME-TIFF on S3 requires an extra install because `tifffile` does not:

```bash
pip install 'iceberg-bioimage[s3]'
```

The reason for the difference: `zarr` already depends on `fsspec`, so `s3://`
paths work immediately. `tifffile` (used for OME-TIFF) does not understand
`fsspec` URLs on its own, so this package adds an `fsspec`-backed file opener
behind the `s3` extra to bridge the gap.

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

Everything above deals with *metadata* — shape, dtype, file location. If you
also need the *pixel data itself* (the "image payload") in Arrow form, for
example to pass into an ML pipeline without re-reading the original file
format, use the optional OME-Arrow integration (requires `ome-arrow >= 0.0.10`):

```python
from iceberg_bioimage import (
    create_ome_arrow_from_tiff,
    create_ome_arrow_from_zarr,
    open_ome_arrow_dataset,
    write_ome_arrow_dataset,
)
```

> **Conversion cost:** `create_ome_arrow_from_zarr`/`create_ome_arrow_from_tiff`
> eagerly read every plane of the source image into memory and re-encode it as
> Arrow — there is no lazy/streaming path. For large images, expect the
> conversion to take roughly as long as reading the full pixel array once, plus
> Arrow encoding overhead. Prefer `open_ome_arrow_dataset` to read an
> already-converted OME-Arrow dataset without re-paying that cost.

Install with `pip install 'iceberg-bioimage[ome-arrow]'`.

______________________________________________________________________

## DuckDB helpers

Optional SQL-style filtering and joins over registered metadata tables,
useful when you want ad-hoc queries (e.g. "all images with `cell_count > 10`")
without writing pandas/pyarrow code by hand:

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
