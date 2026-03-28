# iceberg-bioimage

`iceberg-bioimage` is a format-agnostic Python package for cataloging bioimaging data with Apache Iceberg as the metadata layer.

Core idea:
- Iceberg is the control plane for cataloging, schemas, joins, and snapshots.
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

## Quickstart

```python
from iceberg_bioimage import (
    register_store,
    validate_microscopy_profile_table,
)

registration = register_store("data/experiment.zarr", "default", "bioimage")
print(registration.to_dict())

contract = validate_microscopy_profile_table("data/cells.parquet")
print(contract.is_valid)
```

```bash
iceberg-bioimage scan data/experiment.zarr
iceberg-bioimage register --catalog default --namespace bioimage data/experiment.zarr
iceberg-bioimage publish-chunks --catalog default --namespace bioimage data/experiment.zarr
iceberg-bioimage register --catalog default --namespace bioimage --publish-chunks data/experiment.zarr
iceberg-bioimage validate-contract data/cells.parquet
```

See `examples/quickstart.py` for a minimal script.
See `examples/catalog_duckdb.py` for a catalog-backed query example.
See `examples/synthetic_workflow.py` for a self-contained local workflow.

## DuckDB helpers

DuckDB is supported as an optional integration layer, not as a required engine.

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
filtered = query_metadata_table(joined, where="cell_count > 10")
```

Install the optional integration with `uv sync --group duckdb`.

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
- Publish `image_assets` and `chunk_index` metadata tables with PyIceberg
- Validate profile tables against the microscopy join contract
- Query canonical metadata through optional DuckDB helpers
- Load catalog-backed metadata tables into Arrow for downstream joins
