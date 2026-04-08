# DuckDB Integration

DuckDB is the first supported query integration for `iceberg-bioimage`, but it
is intentionally optional. The core package focuses on scanning,
canonicalization, Cytomining warehouse export, validation, and publishing.

## Install

```bash
uv sync --group duckdb
```

For install alternatives and first-run workflow selection, see [Getting Started](getting-started.md).

## Supported helper functions

- `create_duckdb_connection`
- `query_metadata_table`
- `join_image_assets_with_profiles`
- `load_catalog_table`
- `catalog_table_to_arrow`
- `join_catalog_image_assets_with_profiles`

These helpers operate on canonical metadata in Parquet, Arrow, or row-list
form. Catalog-backed helpers use PyIceberg to materialize canonical metadata
tables into Arrow before querying. None of these helpers replace catalog
management, storage access, or image IO.

For Cytomining workflows, a common pattern is:

1. export `image_assets`, optional `chunk_index`, and optional `joined_profiles`
   into a Parquet warehouse root
1. use `pycytominer` to load those Parquet datasets directly
1. use DuckDB helpers here when you want lightweight SQL over the same metadata

## Example

```python
import pyarrow as pa

from iceberg_bioimage import join_image_assets_with_profiles

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
print(joined.to_pydict())
```

## Catalog-backed example

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
    "bioimage.cytotable",
    profiles,
    chunk_index_table="chunk_index",
)
print(joined.to_pydict())
```
