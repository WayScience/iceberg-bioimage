"""Tests for profile namespace registration and column alias resolution."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from iceberg_bioimage import register_profile_table
from iceberg_bioimage.validation.contracts import (
    resolve_microscopy_profile_columns,
    validate_microscopy_profile_columns,
)
from tests.fakes import FakeCatalog

# ---------------------------------------------------------------------------
# Column alias resolution: NF1 / pycytominer-style _x suffix columns
# ---------------------------------------------------------------------------

NF1_COLUMNS = [
    "Image_FileName_DAPI",
    "Image_FileName_GFP",
    "Image_FileName_RFP",
    "Image_Metadata_Well_x",
    "Image_Metadata_Plate_x",
    "Image_Metadata_Site_x",
    "Nuclei_AreaShape_Area",
    "Cells_AreaShape_Area",
]


def test_nf1_plate_alias_resolved() -> None:
    resolved = resolve_microscopy_profile_columns(NF1_COLUMNS)
    assert resolved["plate_id"] == "Image_Metadata_Plate_x"


def test_nf1_well_alias_resolved() -> None:
    resolved = resolve_microscopy_profile_columns(NF1_COLUMNS)
    assert resolved["well_id"] == "Image_Metadata_Well_x"


def test_nf1_site_alias_resolved() -> None:
    resolved = resolve_microscopy_profile_columns(NF1_COLUMNS)
    assert resolved["site_id"] == "Image_Metadata_Site_x"


def test_nf1_missing_required_columns() -> None:
    result = validate_microscopy_profile_columns(NF1_COLUMNS)
    assert "dataset_id" in result.missing_required_columns
    assert "image_id" in result.missing_required_columns


def test_nf1_recommended_columns_resolved_via_aliases() -> None:
    result = validate_microscopy_profile_columns(NF1_COLUMNS)
    assert "plate_id" not in result.missing_recommended_columns
    assert "well_id" not in result.missing_recommended_columns
    assert "site_id" not in result.missing_recommended_columns


def test_y_suffix_aliases_also_resolve() -> None:
    columns = [
        "Image_Metadata_Well_y",
        "Image_Metadata_Plate_y",
        "Image_Metadata_Site_y",
    ]
    resolved = resolve_microscopy_profile_columns(columns)
    assert resolved["plate_id"] == "Image_Metadata_Plate_y"
    assert resolved["well_id"] == "Image_Metadata_Well_y"
    assert resolved["site_id"] == "Image_Metadata_Site_y"


def test_alias_warning_included_for_resolved_columns() -> None:
    result = validate_microscopy_profile_columns(NF1_COLUMNS)
    warning_text = " ".join(result.warnings)
    assert "alias normalization" in warning_text


# ---------------------------------------------------------------------------
# register_profile_table — basic catalog publishing
# ---------------------------------------------------------------------------

PROFILE_ROWS = [
    {"Image_Metadata_Well_x": "B7", "Image_Metadata_Plate_x": "P01", "CellCount": 42},
    {"Image_Metadata_Well_x": "D6", "Image_Metadata_Plate_x": "P01", "CellCount": 17},
]
PROFILE_TABLE = pa.Table.from_pylist(PROFILE_ROWS)
EXPECTED_PROFILE_ROW_COUNT = 2


def test_register_profile_table_returns_row_count(tmp_path: Path) -> None:
    parquet_path = tmp_path / "profiles.parquet"
    import pyarrow.parquet as pq

    pq.write_table(PROFILE_TABLE, parquet_path)

    catalog = FakeCatalog()
    count = register_profile_table(str(parquet_path), catalog, "nf1.profiles")
    assert count == EXPECTED_PROFILE_ROW_COUNT


def test_register_profile_table_creates_table_in_catalog(tmp_path: Path) -> None:
    parquet_path = tmp_path / "profiles.parquet"
    import pyarrow.parquet as pq

    pq.write_table(PROFILE_TABLE, parquet_path)

    catalog = FakeCatalog()
    register_profile_table(str(parquet_path), catalog, "nf1.profiles")

    assert len(catalog.tables) == 1
    identifier = next(iter(catalog.tables))
    assert identifier[-1] == "profiles"


def test_register_profile_table_custom_table_name(tmp_path: Path) -> None:
    parquet_path = tmp_path / "profiles.parquet"
    import pyarrow.parquet as pq

    pq.write_table(PROFILE_TABLE, parquet_path)

    catalog = FakeCatalog()
    register_profile_table(
        str(parquet_path), catalog, "nf1.profiles", table_name="cell_measurements"
    )

    identifier = next(iter(catalog.tables))
    assert identifier[-1] == "cell_measurements"


def test_register_profile_table_appended_table_has_correct_columns(
    tmp_path: Path,
) -> None:
    parquet_path = tmp_path / "profiles.parquet"
    import pyarrow.parquet as pq

    pq.write_table(PROFILE_TABLE, parquet_path)

    catalog = FakeCatalog()
    register_profile_table(str(parquet_path), catalog, "nf1.profiles")

    fake_table = next(iter(catalog.tables.values()))
    appended = fake_table.appends[0]
    assert "Image_Metadata_Well_x" in appended.schema.names
    assert "Image_Metadata_Plate_x" in appended.schema.names
    assert "CellCount" in appended.schema.names


def test_register_profile_table_dot_namespace_split(tmp_path: Path) -> None:
    parquet_path = tmp_path / "profiles.parquet"
    import pyarrow.parquet as pq

    pq.write_table(PROFILE_TABLE, parquet_path)

    catalog = FakeCatalog()
    register_profile_table(str(parquet_path), catalog, "experiment.nf1.profiles")

    identifier = next(iter(catalog.tables))
    # namespace resolution appends 'cytotable' segment per CytoTable layout
    assert identifier[0] == "experiment"
    assert identifier[1] == "nf1"
    assert identifier[2] == "profiles"
    assert identifier[-1] == "profiles"


def test_register_profile_table_accepts_list_namespace(tmp_path: Path) -> None:
    parquet_path = tmp_path / "profiles.parquet"
    import pyarrow.parquet as pq

    pq.write_table(PROFILE_TABLE, parquet_path)

    catalog = FakeCatalog()
    register_profile_table(str(parquet_path), catalog, ["nf1", "profiles"])

    identifier = next(iter(catalog.tables))
    assert identifier[0] == "nf1"
    assert identifier[1] == "profiles"
    assert identifier[-1] == "profiles"


# ---------------------------------------------------------------------------
# ome-iris NF1 integration: profiles.parquet in the catalog
# ---------------------------------------------------------------------------


@pytest.mark.network
def test_iris_nf1_profiles_registered_to_catalog(iris_nf1: Path) -> None:
    parquet_path = iris_nf1 / "profiles.parquet"
    assert parquet_path.exists(), "Expected profiles.parquet in iris_nf1 download"

    catalog = FakeCatalog()
    count = register_profile_table(str(parquet_path), catalog, "nf1.profiles")
    assert count > 0


@pytest.mark.network
def test_iris_nf1_profiles_have_expected_columns_after_register(iris_nf1: Path) -> None:
    parquet_path = iris_nf1 / "profiles.parquet"

    catalog = FakeCatalog()
    register_profile_table(str(parquet_path), catalog, "nf1.profiles")

    fake_table = next(iter(catalog.tables.values()))
    appended = fake_table.appends[0]
    schema_names = set(appended.schema.names)

    assert "Image_Metadata_Well_x" in schema_names
    assert "Image_Metadata_Plate_x" in schema_names


@pytest.mark.network
def test_iris_nf1_profiles_alias_resolves_well_and_site(iris_nf1: Path) -> None:
    import pyarrow.dataset as ds

    parquet_path = iris_nf1 / "profiles.parquet"
    columns = ds.dataset(str(parquet_path)).schema.names

    resolved = resolve_microscopy_profile_columns(columns)
    assert resolved["well_id"] is not None, "well_id alias should resolve"
    assert resolved["plate_id"] is not None, "plate_id alias should resolve"
