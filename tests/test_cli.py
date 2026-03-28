"""CLI tests."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import zarr
from pytest import CaptureFixture, MonkeyPatch

from iceberg_bioimage import cli as cli_module

CLI_VALUE_ERROR_EXIT_CODE = 2


def test_scan_cli(tmp_path: Path) -> None:
    store_path = tmp_path / "demo.zarr"
    root = zarr.open_group(store_path, mode="w")
    data = np.arange(6, dtype=np.uint8).reshape(2, 3)
    root.create_dataset("0", shape=data.shape, data=data, chunks=(1, 3))

    output = subprocess.run(
        [sys.executable, "-m", "iceberg_bioimage.cli", "scan", str(store_path)],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )

    assert "format_family: zarr" in output.stdout
    assert "shape=[2, 3]" in output.stdout


def test_validate_contract_cli(tmp_path: Path) -> None:
    table_path = tmp_path / "cells.parquet"
    pq.write_table(
        pa.table(
            {
                "dataset_id": ["ds-1"],
                "image_id": ["img-1"],
                "value": [1],
            }
        ),
        table_path,
    )

    output = subprocess.run(
        [
            sys.executable,
            "-m",
            "iceberg_bioimage.cli",
            "validate-contract",
            str(table_path),
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )

    assert "is_valid: True" in output.stdout
    assert "missing_recommended_columns:" in output.stdout


def test_register_cli_publish_chunks(
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    def _fake_register_store(
        uri: str,
        catalog: str,
        namespace: str,
        *,
        image_assets_table: str = "image_assets",
        chunk_index_table: str | None = "chunk_index",
    ) -> object:
        assert uri == "data/example.zarr"
        assert catalog == "default"
        assert namespace == "bioimage"
        assert image_assets_table == "image_assets"
        assert chunk_index_table == "chunk_index"

        return SimpleNamespace(
            image_assets_rows_published=1,
            chunk_rows_published=4,
            source_uri=uri,
        )

    monkeypatch.setattr(
        cli_module,
        "register_store",
        _fake_register_store,
    )

    exit_code = cli_module.main(
        [
            "register",
            "--catalog",
            "default",
            "--namespace",
            "bioimage",
            "--publish-chunks",
            "data/example.zarr",
        ]
    )
    output = capsys.readouterr()

    assert exit_code == 0
    assert '"chunk_rows_published": 4' in output.out


def test_publish_chunks_cli(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    store_path = tmp_path / "demo.zarr"
    root = zarr.open_group(store_path, mode="w")
    data = np.arange(6, dtype=np.uint8).reshape(2, 3)
    root.create_dataset("0", shape=data.shape, data=data, chunks=(1, 3))

    def _fake_publish_chunk_index(
        catalog: str,
        namespace: str,
        table_name: str,
        scan_result: object,
    ) -> int:
        assert catalog == "default"
        assert namespace == "bioimage"
        assert table_name == "chunk_index"
        assert getattr(scan_result, "source_uri", None) == str(store_path)
        return 2

    monkeypatch.setattr(
        cli_module,
        "publish_chunk_index",
        _fake_publish_chunk_index,
    )

    exit_code = cli_module.main(
        [
            "publish-chunks",
            "--catalog",
            "default",
            "--namespace",
            "bioimage",
            str(store_path),
        ]
    )
    output = capsys.readouterr()

    assert exit_code == 0
    assert '"rows_published": 2' in output.out


def test_main_returns_cli_error_for_value_error(
    monkeypatch: MonkeyPatch,
    capsys: CaptureFixture[str],
) -> None:
    def raise_value_error(args: object) -> int:
        raise ValueError("bad dataset")

    monkeypatch.setattr(
        cli_module,
        "_handle_scan",
        raise_value_error,
    )

    exit_code = cli_module.main(["scan", "data/missing.zarr"])
    output = capsys.readouterr()

    assert exit_code == CLI_VALUE_ERROR_EXIT_CODE
    assert "Error: bad dataset" in output.err
