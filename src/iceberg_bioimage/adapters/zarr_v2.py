"""Zarr v2 adapter."""

from __future__ import annotations

from pathlib import Path

import zarr

from iceberg_bioimage.adapters.base import BaseAdapter
from iceberg_bioimage.models.scan_result import ImageAsset, ScanResult


class ZarrV2Adapter(BaseAdapter):
    """Scan `.zarr` stores into canonical image assets."""

    name = "zarr-v2"
    format_family = "zarr"

    def can_handle(self, uri: str) -> bool:
        return uri.lower().endswith(".zarr") or ".zarr/" in uri.lower()

    def scan(self, uri: str) -> ScanResult:
        store = zarr.open(uri, mode="r")
        image_assets: list[ImageAsset] = []

        if hasattr(store, "shape") and hasattr(store, "dtype"):
            image_assets.append(
                self._build_asset(uri=uri, array_path=None, array=store)
            )
        elif hasattr(store, "visititems"):
            store.visititems(
                lambda path, node: self._maybe_collect_array(
                    uri,
                    image_assets,
                    path,
                    node,
                )
            )

        if not image_assets:
            raise ValueError(f"No arrays were discovered in Zarr store {uri!r}.")

        return ScanResult(
            source_uri=uri,
            format_family=self.format_family,
            image_assets=image_assets,
        )

    def _maybe_collect_array(
        self,
        uri: str,
        image_assets: list[ImageAsset],
        array_path: str,
        node: object,
    ) -> None:
        if hasattr(node, "shape") and hasattr(node, "dtype"):
            image_assets.append(
                self._build_asset(uri=uri, array_path=array_path, array=node)
            )

    def _build_asset(
        self,
        uri: str,
        array_path: str | None,
        array: object,
    ) -> ImageAsset:
        path = array_path or None
        metadata = {"store_name": Path(uri).name}

        return ImageAsset(
            uri=uri,
            array_path=path,
            shape=[int(value) for value in getattr(array, "shape")],
            dtype=str(getattr(array, "dtype")),
            chunk_shape=self._coerce_chunks(getattr(array, "chunks", None)),
            metadata=metadata,
            image_id=self._image_id(uri, path),
        )

    def _coerce_chunks(self, chunks: object) -> list[int] | None:
        if not chunks:
            return None

        return [int(value) for value in chunks]

    def _image_id(self, uri: str, array_path: str | None) -> str:
        stem = Path(uri.rstrip("/")).name.removesuffix(".zarr")
        return stem if array_path is None else f"{stem}:{array_path}"
