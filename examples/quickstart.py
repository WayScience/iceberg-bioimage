"""Minimal local usage example."""

from __future__ import annotations

from iceberg_bioimage import (
    register_store,
    validate_microscopy_profile_table,
)


def run_example(
    uri: str,
    catalog_name: str,
    namespace: str,
    profile_table: str | None = None,
) -> None:
    registration = register_store(uri, catalog_name, namespace)
    print(registration.to_json(indent=2, sort_keys=True))

    if profile_table is not None:
        validation_result = validate_microscopy_profile_table(profile_table)
        print(validation_result.to_json(indent=2, sort_keys=True))


if __name__ == "__main__":
    run_example("data/example.zarr", "default", "bioimage")
