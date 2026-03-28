"""Shared test doubles."""

from __future__ import annotations

import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError


class FakeTable:
    """Simple append-only table stub."""

    def __init__(self) -> None:
        self.appends: list[pa.Table] = []

    def append(self, table: pa.Table) -> None:
        self.appends.append(table)


class FakeCatalog:
    """Minimal catalog stub."""

    def __init__(self, table: FakeTable | None = None) -> None:
        self.table = table
        self.created_identifiers: list[tuple[str, ...]] = []

    def load_table(self, identifier: tuple[str, ...]) -> FakeTable:
        if self.table is None:
            raise NoSuchTableError(f"Missing table: {identifier!r}")
        return self.table

    def create_table(self, identifier: tuple[str, ...], schema: object) -> FakeTable:
        self.created_identifiers.append(identifier)
        self.table = FakeTable()
        return self.table
