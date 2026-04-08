# Catalog Setup

Catalog-backed commands use PyIceberg catalogs.

Commands that require catalog configuration:

- `register`
- `ingest`
- `publish-chunks`
- `export-cytomining-catalog`

## Catalog Name vs Catalog Object

You can pass either:

- a catalog name string, such as `"default"` (resolved by PyIceberg configuration)
- a catalog object instance

Example with a catalog object:

```python
from pyiceberg.catalog import load_catalog

from iceberg_bioimage import register_store

catalog = load_catalog("default")
result = register_store(
    "data/experiment.zarr",
    catalog,
    "bioimage.cytotable",
)
print(result.to_dict())
```

## Recommended Namespace

For new projects, use:

- `bioimage.cytotable`

The library also supports fallback to legacy layouts where canonical tables already exist directly under `bioimage`.

## Verify Catalog Access

Test catalog-backed listing with:

```python
from iceberg_bioimage import list_catalog_tables

tables = list_catalog_tables("default", "bioimage.cytotable")
print(tables)
```

If this fails, validate your PyIceberg catalog settings and backend credentials before running publish/export commands.
