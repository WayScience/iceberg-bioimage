"""Tests for microscopy contract validation."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from iceberg_bioimage.validation.contracts import (
    validate_microscopy_profile_columns,
    validate_microscopy_profile_table,
)


def test_validate_microscopy_profile_columns_valid() -> None:
    result = validate_microscopy_profile_columns(
        ["dataset_id", "image_id", "plate_id"],
        target="cells",
    )

    assert result.is_valid is True
    assert result.missing_required_columns == []
    assert result.missing_recommended_columns == ["well_id", "site_id"]


def test_validate_microscopy_profile_table(tmp_path: Path) -> None:
    table_path = tmp_path / "cells.parquet"
    table = pa.table(
        {
            "dataset_id": ["ds-1"],
            "image_id": ["img-1"],
            "intensity": [1.0],
        }
    )
    pq.write_table(table, table_path)

    result = validate_microscopy_profile_table(str(table_path))

    assert result.is_valid is True
    assert result.target == str(table_path)
    assert result.present_columns == ["dataset_id", "image_id", "intensity"]


def test_validate_microscopy_profile_columns_invalid() -> None:
    result = validate_microscopy_profile_columns(["plate_id", "well_id"])

    assert result.is_valid is False
    assert result.missing_required_columns == ["dataset_id", "image_id"]
