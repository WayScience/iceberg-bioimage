"""Shared test doubles."""

from __future__ import annotations

import pyarrow as pa
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError


class FakeTable:
    """Simple append-only table stub."""

    def __init__(self) -> None:
        self.appends: list[pa.Table] = []

    def append(self, table: pa.Table) -> None:
        self.appends.append(table)


class _BaseFakeCatalog:
    """Shared table-handling behavior for fake catalogs."""

    def __init__(
        self,
        table: FakeTable | None = None,
        tables: dict[tuple[str, ...], FakeTable] | None = None,
    ) -> None:
        self.table = table
        self.tables = {} if tables is None else dict(tables)
        self.created_identifiers: list[tuple[str, ...]] = []
        self.created_namespaces: list[tuple[str, ...]] = []

    def load_table(self, identifier: tuple[str, ...]) -> FakeTable:
        if identifier in self.tables:
            return self.tables[identifier]
        if self.table is None:
            raise NoSuchTableError(f"Missing table: {identifier!r}")
        return self.table

    def create_table(self, identifier: tuple[str, ...], schema: object) -> FakeTable:
        self.created_identifiers.append(identifier)
        self.table = FakeTable()
        self.tables[identifier] = self.table
        return self.table

    def create_namespace_if_not_exists(self, namespace: tuple[str, ...]) -> None:
        self.created_namespaces.append(namespace)

    def list_tables(self, namespace: tuple[str, ...]) -> list[tuple[str, ...]]:
        return [
            identifier for identifier in self.tables if identifier[:-1] == namespace
        ]


class FakeCatalog(_BaseFakeCatalog):
    """Minimal catalog stub."""

    def create_namespace_if_not_exists(self, namespace: tuple[str, ...]) -> None:
        self.created_namespaces.append(namespace)


class FakeCreateNamespaceCatalog(_BaseFakeCatalog):
    """Catalog stub that only exposes create_namespace."""

    def __init__(
        self,
        table: FakeTable | None = None,
        tables: dict[tuple[str, ...], FakeTable] | None = None,
    ) -> None:
        super().__init__(table=table, tables=tables)
        self.namespaces: set[tuple[str, ...]] = set()

    def create_namespace(self, namespace: tuple[str, ...]) -> None:
        if namespace in self.namespaces:
            raise NamespaceAlreadyExistsError(
                f"Namespace already exists: {namespace!r}"
            )
        self.namespaces.add(namespace)
        self.created_namespaces.append(namespace)
