"""Validation helper tests."""

from __future__ import annotations

from pathlib import Path

from iceberg_bioimage.validation.contracts import validate_microscopy_profile_table


def test_validate_microscopy_profile_table_invalid_path() -> None:
    result = validate_microscopy_profile_table("/tmp/does-not-exist.parquet")

    assert result.is_valid is False
    assert result.target == str(Path("/tmp/does-not-exist.parquet"))
    assert any("Invalid dataset path" in warning for warning in result.warnings)
