"""Adapter base contract."""

from __future__ import annotations

from abc import ABC, abstractmethod

from iceberg_bioimage.models.scan_result import ScanResult


class BaseAdapter(ABC):
    """Base contract for format adapters."""

    name: str = "base"
    format_family: str = "unknown"

    @abstractmethod
    def can_handle(self, uri: str) -> bool:
        """Return whether the adapter can scan the given URI."""

    @abstractmethod
    def scan(self, uri: str) -> ScanResult:
        """Scan the URI and return canonical metadata."""
