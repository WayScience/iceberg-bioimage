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

# # Metadata Workflow Example
#
# This notebook demonstrates the stable, user-facing API without requiring a
# live catalog. It covers three common tasks:
#
# - build a canonical `ScanResult`
# - summarize it into a user-facing `DatasetSummary`
# - join it to profile data in Arrow
#

# +
import pyarrow as pa

from iceberg_bioimage import (
    ImageAsset,
    ScanResult,
    join_profiles_with_scan_result,
    summarize_scan_result,
)
from iceberg_bioimage.publishing.chunk_index import scan_result_to_chunk_rows
from iceberg_bioimage.publishing.image_assets import scan_result_to_rows

# +
scan_result = ScanResult(
    source_uri="data/example.ome.zarr",
    format_family="zarr",
    image_assets=[
        ImageAsset(
            uri="data/example.ome.zarr",
            array_path="0",
            shape=[1, 1, 256, 256],
            dtype="uint16",
            chunk_shape=[1, 1, 128, 128],
            metadata={
                "axes": "czyx",
                "channel_count": 1,
                "storage_variant": "zarr-v2",
            },
            image_id="example:0",
        )
    ],
)

scan_result.to_dict()
# -

summary = summarize_scan_result(scan_result)
summary.to_dict()

# +
profiles = pa.table(
    {
        "dataset_id": ["example"],
        "image_id": ["example:0"],
        "cell_count": [42],
    }
)

joined = join_profiles_with_scan_result(scan_result, profiles, include_chunks=True)
joined.to_pydict()
# -

{
    "image_assets": scan_result_to_rows(scan_result),
    "chunk_index_count": len(scan_result_to_chunk_rows(scan_result)),
}

# ## Optional OME-Arrow path
#
# If you install the optional `ome-arrow` extra, the package also exposes a
# small bridge for Arrow-native image payload workflows:
#
# ```python
# from iceberg_bioimage import create_ome_arrow, scan_ome_arrow
#
# oa = create_ome_arrow("image.ome.tiff")
# lazy_oa = scan_ome_arrow("image.ome.parquet")
# ```
#
# That keeps metadata registration and Arrow-native image handling adjacent,
# without pulling OME-Arrow into the required dependency set.
#
