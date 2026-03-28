"""Validation helpers for canonical scan objects and join contracts."""

from __future__ import annotations

from pathlib import Path

import pyarrow.dataset as ds

from iceberg_bioimage.models.scan_result import (
    ContractValidationResult,
    ScanResult,
)

MICROSCOPY_REQUIRED_JOIN_KEYS = ("dataset_id", "image_id")
MICROSCOPY_RECOMMENDED_JOIN_KEYS = ("plate_id", "well_id", "site_id")


def validate_scan_result(scan_result: ScanResult) -> list[str]:
    """Return validation errors for a scan result."""

    errors: list[str] = []
    if not scan_result.source_uri:
        errors.append("ScanResult.source_uri is required.")
    if not scan_result.image_assets:
        errors.append("ScanResult.image_assets must contain at least one asset.")

    for index, asset in enumerate(scan_result.image_assets):
        prefix = f"image_assets[{index}]"
        if not asset.uri:
            errors.append(f"{prefix}.uri is required.")
        if not asset.shape:
            errors.append(f"{prefix}.shape is required.")
        if not asset.dtype:
            errors.append(f"{prefix}.dtype is required.")

    return errors


def raise_for_invalid_scan_result(scan_result: ScanResult) -> None:
    """Raise a ValueError when a scan result is invalid."""

    errors = validate_scan_result(scan_result)
    if errors:
        raise ValueError("Invalid ScanResult: " + "; ".join(errors))


def validate_microscopy_profile_columns(
    columns: list[str] | tuple[str, ...],
    *,
    target: str = "profile_table",
) -> ContractValidationResult:
    """Validate a schema against the microscopy join contract."""

    present_columns = list(columns)
    missing_required = [
        column
        for column in MICROSCOPY_REQUIRED_JOIN_KEYS
        if column not in present_columns
    ]
    missing_recommended = [
        column
        for column in MICROSCOPY_RECOMMENDED_JOIN_KEYS
        if column not in present_columns
    ]

    warnings: list[str] = []
    if missing_recommended:
        warnings.append(
            "Recommended microscopy join keys are missing: "
            + ", ".join(missing_recommended)
        )

    return ContractValidationResult(
        target=target,
        present_columns=present_columns,
        required_columns=list(MICROSCOPY_REQUIRED_JOIN_KEYS),
        recommended_columns=list(MICROSCOPY_RECOMMENDED_JOIN_KEYS),
        missing_required_columns=missing_required,
        missing_recommended_columns=missing_recommended,
        warnings=warnings,
    )


def validate_microscopy_profile_table(path: str) -> ContractValidationResult:
    """Validate a local profile table file against the microscopy join contract."""

    dataset = ds.dataset(path)
    return validate_microscopy_profile_columns(
        list(dataset.schema.names),
        target=str(Path(path)),
    )
