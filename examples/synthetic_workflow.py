"""Self-contained local workflow for onboarding and demos."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import zarr

from iceberg_bioimage import (
    join_image_assets_with_profiles,
    scan_store,
    validate_microscopy_profile_table,
)
from iceberg_bioimage.publishing.chunk_index import scan_result_to_chunk_rows
from iceberg_bioimage.publishing.image_assets import scan_result_to_rows


def run_example() -> None:
    """Run a synthetic local workflow without requiring a live catalog."""

    with TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        image_store = root / "synthetic_plate.zarr"
        profiles_path = root / "cells.parquet"

        _write_demo_zarr(image_store)
        _write_demo_profiles(profiles_path)

        scan_result = scan_store(str(image_store))
        validation = validate_microscopy_profile_table(str(profiles_path))
        image_assets = pa.Table.from_pylist(scan_result_to_rows(scan_result))
        chunk_index = pa.Table.from_pylist(scan_result_to_chunk_rows(scan_result))
        profiles = pq.read_table(profiles_path)

        joined = join_image_assets_with_profiles(
            image_assets,
            profiles,
            chunk_index=chunk_index,
        )

        print("Scan result")
        print(scan_result.to_json(indent=2, sort_keys=True))
        print("Contract validation")
        print(validation.to_json(indent=2, sort_keys=True))
        print("Joined rows")
        print(joined.to_pydict())


def _write_demo_zarr(path: Path) -> None:
    root = zarr.open_group(path, mode="w")
    data = np.arange(24, dtype=np.uint16).reshape(2, 3, 4)
    root.create_dataset("0", shape=data.shape, data=data, chunks=(1, 3, 2))


def _write_demo_profiles(path: Path) -> None:
    table = pa.table(
        {
            "dataset_id": ["synthetic_plate"],
            "image_id": ["synthetic_plate:0"],
            "cell_count": [12],
            "mean_intensity": [128.5],
        }
    )
    pq.write_table(table, path)


if __name__ == "__main__":
    run_example()
