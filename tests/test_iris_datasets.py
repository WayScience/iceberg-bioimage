"""Integration tests using real ome-iris datasets.

These tests download small sample datasets from the internet and verify that
iceberg-bioimage can scan, register, and query them end-to-end.

Run with: pytest -m network
Skip with: pytest -m 'not network'  (default)
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pytest

from iceberg_bioimage import (
    register_directory,
    scan_as_arrow_table,
    scan_store,
    summarize_store,
)
from tests.fakes import FakeCatalog

# ---------------------------------------------------------------------------
# nuclei-3d (OME-TIFF images + CSV profile)
# ---------------------------------------------------------------------------


@pytest.mark.network
def test_iris_nuclei_scan_single_tiff(iris_nuclei_3d: Path) -> None:
    """Scanning one ome-iris TIFF returns a valid ScanResult."""

    tiff_files = sorted((iris_nuclei_3d / "images").glob("*.tif"))
    assert tiff_files, "No .tif files found in iris_nuclei_3d/images"

    result = scan_store(str(tiff_files[0]))

    assert result.format_family == "ome-tiff"
    assert len(result.image_assets) >= 1
    asset = result.image_assets[0]
    assert asset.shape  # non-empty shape
    assert asset.dtype  # non-empty dtype


@pytest.mark.network
def test_iris_nuclei_scan_as_arrow_table(iris_nuclei_3d: Path) -> None:
    """scan_as_arrow_table on an ome-iris TIFF returns a queryable Arrow table."""

    tiff_files = sorted((iris_nuclei_3d / "images").glob("*.tif"))
    assert tiff_files

    table = scan_as_arrow_table(str(tiff_files[0]))

    assert isinstance(table, pa.Table)
    assert table.num_rows >= 1
    assert "dataset_id" in table.schema.names
    assert "image_id" in table.schema.names
    assert "format_family" in table.schema.names
    assert "shape_json" in table.schema.names
    assert table["format_family"][0].as_py() == "ome-tiff"


@pytest.mark.network
def test_iris_nuclei_shape_is_valid_json(iris_nuclei_3d: Path) -> None:
    """shape_json in the Arrow table deserializes to a non-empty list of ints."""

    tiff_files = sorted((iris_nuclei_3d / "images").glob("*.tif"))
    table = scan_as_arrow_table(str(tiff_files[0]))

    shape = json.loads(table["shape_json"][0].as_py())
    assert isinstance(shape, list)
    assert all(isinstance(dim, int) and dim > 0 for dim in shape)


@pytest.mark.network
def test_iris_nuclei_summarize_store(iris_nuclei_3d: Path) -> None:
    """summarize_store succeeds on an ome-iris TIFF."""

    tiff_files = sorted((iris_nuclei_3d / "images").glob("*.tif"))
    summary = summarize_store(str(tiff_files[0]))

    assert summary.format_family == "ome-tiff"
    assert summary.image_asset_count >= 1
    assert summary.dtypes  # non-empty


@pytest.mark.network
def test_iris_nuclei_register_directory(iris_nuclei_3d: Path) -> None:
    """register_directory on the images folder registers all TIFF files."""

    images_dir = iris_nuclei_3d / "images"
    tiff_count = len(list(images_dir.glob("*.tif")))
    assert tiff_count > 0

    catalog = FakeCatalog()
    result = register_directory(
        str(images_dir),
        catalog,
        "iris.cytotable",
        glob="**/*.tif",
    )

    assert result.dataset_count == tiff_count
    assert result.image_assets_rows_published == tiff_count


@pytest.mark.network
def test_iris_nuclei_register_directory_replace(iris_nuclei_3d: Path) -> None:
    """register_directory with replace=True re-registers without accumulating rows."""

    images_dir = iris_nuclei_3d / "images"
    catalog = FakeCatalog()

    register_directory(str(images_dir), catalog, "iris.cytotable", glob="**/*.tif")
    register_directory(
        str(images_dir),
        catalog,
        "iris.cytotable",
        glob="**/*.tif",
        replace=True,
    )

    # Each replace call deleted + re-appended — FakeTable tracks both
    image_table = next(iter(catalog.tables.values()))
    # delete was called once per dataset on the second pass
    assert len(image_table.deletes) > 0


@pytest.mark.network
def test_iris_nuclei_masks_also_scannable(iris_nuclei_3d: Path) -> None:
    """Mask TIFFs from the nuclei-3d dataset are scannable as ome-tiff."""

    mask_files = sorted((iris_nuclei_3d / "masks").glob("*.tiff"))
    assert mask_files, "No .tiff mask files found"

    for mask_path in mask_files:
        table = scan_as_arrow_table(str(mask_path))
        assert table.num_rows >= 1
        assert table["format_family"][0].as_py() == "ome-tiff"


# ---------------------------------------------------------------------------
# nf1 (OME-TIFF images + Parquet profile)
# ---------------------------------------------------------------------------


@pytest.mark.network
def test_iris_nf1_scan_images(iris_nf1: Path) -> None:
    """Scanning nf1 ome-iris images returns ome-tiff ScanResults."""

    tiff_files = sorted((iris_nf1 / "images").glob("*.tif"))
    assert tiff_files, "No .tif files in nf1/images"

    for tiff_path in tiff_files:
        result = scan_store(str(tiff_path))
        assert result.format_family == "ome-tiff"
        assert result.image_assets


@pytest.mark.network
def test_iris_nf1_scan_as_arrow_table(iris_nf1: Path) -> None:
    """scan_as_arrow_table on nf1 images returns valid Arrow tables."""

    tiff_files = sorted((iris_nf1 / "images").glob("*.tif"))
    assert tiff_files, "No .tif files found in iris_nf1/images"
    tables = [scan_as_arrow_table(str(p)) for p in tiff_files]

    assert all(isinstance(t, pa.Table) for t in tables)
    assert all(t.num_rows >= 1 for t in tables)


@pytest.mark.network
def test_iris_nf1_register_directory(iris_nf1: Path) -> None:
    """register_directory discovers all nf1 images in one call."""

    images_dir = iris_nf1 / "images"
    tiff_count = len(list(images_dir.glob("*.tif")))

    catalog = FakeCatalog()
    result = register_directory(
        str(images_dir),
        catalog,
        "nf1.cytotable",
        glob="**/*.tif",
    )

    assert result.dataset_count == tiff_count
    assert result.namespace == ["nf1", "cytotable"]


@pytest.mark.network
def test_iris_nf1_dataset_ids_are_unique(iris_nf1: Path) -> None:
    """Each nf1 TIFF produces a distinct dataset_id in the Arrow table."""

    tiff_files = sorted((iris_nf1 / "images").glob("*.tif"))
    assert tiff_files, "No .tif files found in iris_nf1/images"
    dataset_ids = [
        scan_as_arrow_table(str(p))["dataset_id"][0].as_py() for p in tiff_files
    ]

    assert len(dataset_ids) == len(set(dataset_ids)), "Duplicate dataset_ids found"


@pytest.mark.network
def test_iris_nf1_profiles_parquet_is_present(iris_nf1: Path) -> None:
    """The nf1 dataset includes a Parquet profile file."""

    assert (iris_nf1 / "profiles.parquet").exists(), "profiles.parquet missing"


@pytest.mark.network
def test_iris_nf1_scan_and_register_combined(iris_nf1: Path) -> None:
    """Full pipeline: scan images + register to catalog in a single call."""

    images_dir = iris_nf1 / "images"
    catalog = FakeCatalog()

    result = register_directory(
        str(images_dir),
        catalog,
        "nf1.cytotable",
        glob="**/*.tif",
        chunk_index_table=None,
    )

    assert result.dataset_count >= 1
    assert result.image_assets_rows_published >= 1
    assert result.chunk_rows_published == 0
