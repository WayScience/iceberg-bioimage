"""Example of querying catalog-backed metadata with optional DuckDB helpers."""

from __future__ import annotations

import pyarrow as pa

from iceberg_bioimage import join_catalog_image_assets_with_profiles


def run_example(catalog_name: str, namespace: str) -> None:
    profiles = pa.table(
        {
            "dataset_id": ["ds-1"],
            "image_id": ["img-1"],
            "cell_count": [42],
        }
    )
    joined = join_catalog_image_assets_with_profiles(
        catalog_name,
        namespace,
        profiles,
        chunk_index_table="chunk_index",
    )
    print(joined.to_pydict())


if __name__ == "__main__":
    run_example("default", "bioimage")
