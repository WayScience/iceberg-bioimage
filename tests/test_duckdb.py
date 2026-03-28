"""Tests for optional DuckDB helpers."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pytest.importorskip("duckdb")

from iceberg_bioimage.integrations.duckdb import (
    create_duckdb_connection,
    join_image_assets_with_profiles,
    query_metadata_table,
)


def test_query_metadata_table_from_arrow() -> None:
    table = pa.table(
        {
            "dataset_id": ["ds-1", "ds-2"],
            "image_id": ["img-1", "img-2"],
            "cell_count": [42, 3],
        }
    )

    result = query_metadata_table(
        table,
        columns=["dataset_id", "cell_count"],
        filters=[("cell_count", ">", 10)],
    )

    assert result.to_pydict() == {
        "dataset_id": ["ds-1"],
        "cell_count": [42],
    }


def test_query_metadata_table_from_parquet(tmp_path: Path) -> None:
    table_path = tmp_path / "image_assets.parquet"
    pq.write_table(
        pa.table(
            {
                "dataset_id": ["ds-1"],
                "image_id": ["img-1"],
                "uri": ["data/example.zarr"],
            }
        ),
        table_path,
    )

    result = query_metadata_table(table_path)

    assert result.to_pydict()["image_id"] == ["img-1"]


def test_query_metadata_table_rejects_unknown_columns() -> None:
    table = pa.table({"dataset_id": ["ds-1"]})

    with pytest.raises(ValueError, match="Unknown filter column"):
        query_metadata_table(table, filters=[("missing", "=", "value")])


def test_join_image_assets_with_profiles_and_chunks() -> None:
    image_assets = pa.table(
        {
            "dataset_id": ["ds-1"],
            "image_id": ["img-1"],
            "array_path": ["0"],
            "uri": ["data/example.zarr"],
        }
    )
    profiles = pa.table(
        {
            "dataset_id": ["ds-1"],
            "image_id": ["img-1"],
            "cell_count": [42],
        }
    )
    chunk_index = pa.table(
        {
            "dataset_id": ["ds-1"],
            "image_id": ["img-1"],
            "array_path": ["0"],
            "chunk_key": ["0/0"],
            "chunk_coords_json": ["[0, 0]"],
            "byte_length": [1024],
        }
    )

    result = join_image_assets_with_profiles(
        image_assets,
        profiles,
        chunk_index=chunk_index,
    )

    assert result.to_pydict() == {
        "dataset_id": ["ds-1"],
        "image_id": ["img-1"],
        "array_path": ["0"],
        "uri": ["data/example.zarr"],
        "cell_count": [42],
        "chunk_key": ["0/0"],
        "chunk_coords_json": ["[0, 0]"],
        "byte_length": [1024],
    }


def test_create_duckdb_connection() -> None:
    connection = create_duckdb_connection()
    result = connection.execute("SELECT 1 AS value").arrow().read_all()
    connection.close()

    assert result.to_pydict() == {"value": [1]}
