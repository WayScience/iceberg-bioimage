"""Zarr adapter for local Zarr v2 stores and local Zarr v3 metadata stores."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlparse

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
        if self._is_local_zarr_v3(uri):
            return self._scan_local_zarr_v3(uri)

        store = zarr.open(uri, mode="r")
        image_assets: list[ImageAsset] = []
        root_attrs = getattr(store, "attrs", None)

        if hasattr(store, "shape") and hasattr(store, "dtype"):
            image_assets.append(
                self._build_asset(
                    uri=uri,
                    array_path=None,
                    array=store,
                    root_attrs=root_attrs,
                    storage_variant="zarr-v2",
                )
            )
        else:
            self._collect_group_arrays(
                uri=uri,
                image_assets=image_assets,
                node=store,
                root_attrs=root_attrs,
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
        root_attrs: object,
    ) -> None:
        if hasattr(node, "shape") and hasattr(node, "dtype"):
            image_assets.append(
                self._build_asset(
                    uri=uri,
                    array_path=array_path,
                    array=node,
                    root_attrs=root_attrs,
                    storage_variant="zarr-v2",
                )
            )

    def _collect_group_arrays(
        self,
        uri: str,
        image_assets: list[ImageAsset],
        node: object,
        root_attrs: object,
        *,
        prefix: str = "",
    ) -> None:
        if hasattr(node, "visititems"):
            node.visititems(
                lambda path, child: self._maybe_collect_array(
                    uri,
                    image_assets,
                    path,
                    child,
                    root_attrs,
                )
            )
            return

        for key in self._node_keys(node):
            child = node[key]
            path = f"{prefix}/{key}" if prefix else str(key)
            if hasattr(child, "shape") and hasattr(child, "dtype"):
                self._maybe_collect_array(
                    uri,
                    image_assets,
                    path,
                    child,
                    root_attrs,
                )
                continue
            self._collect_group_arrays(
                uri=uri,
                image_assets=image_assets,
                node=child,
                root_attrs=root_attrs,
                prefix=path,
            )

    def _node_keys(self, node: object) -> list[str]:
        keys = getattr(node, "keys", None)
        if callable(keys):
            return [str(key) for key in keys()]
        return []

    def _build_asset(
        self,
        uri: str,
        array_path: str | None,
        array: object,
        root_attrs: object,
        storage_variant: str,
    ) -> ImageAsset:
        path = None if array_path == "" else array_path
        metadata = {
            "store_name": Path(uri).name,
            "storage_variant": storage_variant,
            "ndim": len(getattr(array, "shape")),
        }
        metadata.update(self._extract_axes_metadata(path, root_attrs))
        metadata["channel_count"] = self._channel_count_from_axes(
            metadata.get("axes"),
            getattr(array, "shape"),
        )

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
        name = Path(uri.rstrip("/")).name
        normalized = name.casefold()
        if normalized.endswith(".ome.zarr"):
            stem = name[:-9]
        elif normalized.endswith(".zarr"):
            stem = name[:-5]
        else:
            stem = name
        return stem if array_path is None else f"{stem}:{array_path}"

    def _is_local_zarr_v3(self, uri: str) -> bool:
        """Return whether ``uri`` is a local Zarr v3 metadata store."""

        parsed = urlparse(uri)
        if parsed.scheme not in {"", "file"}:
            return False
        path = Path(uri)
        return path.is_dir() and (path / "zarr.json").exists()

    def _scan_local_zarr_v3(self, uri: str) -> ScanResult:
        root = Path(uri)
        image_assets: list[ImageAsset] = []
        root_metadata = json.loads((root / "zarr.json").read_text())
        root_attributes = root_metadata.get("attributes", {})

        for metadata_path in sorted(root.rglob("zarr.json")):
            node_metadata = json.loads(metadata_path.read_text())
            if node_metadata.get("node_type") != "array":
                continue

            array_dir = metadata_path.parent
            relative_path = array_dir.relative_to(root).as_posix()
            array_path = None if relative_path == "." else relative_path
            metadata = {
                "store_name": root.name,
                "storage_variant": "zarr-v3",
                "ndim": len(node_metadata.get("shape", [])),
            }
            metadata.update(self._extract_axes_metadata(array_path, root_attributes))
            metadata["channel_count"] = self._channel_count_from_axes(
                metadata.get("axes"),
                node_metadata.get("shape", []),
            )

            image_assets.append(
                ImageAsset(
                    uri=uri,
                    array_path=array_path,
                    shape=[int(value) for value in node_metadata.get("shape", [])],
                    dtype=self._coerce_v3_dtype(node_metadata.get("data_type")),
                    chunk_shape=self._coerce_v3_chunk_shape(
                        node_metadata.get("chunk_grid")
                    ),
                    metadata=metadata,
                    image_id=self._image_id(uri, array_path),
                )
            )

        if not image_assets:
            raise ValueError(f"No arrays were discovered in Zarr store {uri!r}.")

        return ScanResult(
            source_uri=uri,
            format_family=self.format_family,
            image_assets=image_assets,
        )

    def _extract_axes_metadata(
        self,
        array_path: str | None,
        root_attrs: object,
    ) -> dict[str, object]:
        axes = None
        multiscales = None
        if isinstance(root_attrs, Mapping) or hasattr(root_attrs, "get"):
            multiscales = root_attrs.get("multiscales")
        if isinstance(multiscales, list):
            for multiscale in multiscales:
                datasets = multiscale.get("datasets", [])
                if not isinstance(datasets, list):
                    continue
                dataset_paths = {dataset.get("path") for dataset in datasets}
                normalized_path = "" if array_path is None else array_path
                if normalized_path in dataset_paths:
                    axes_value = multiscale.get("axes")
                    if isinstance(axes_value, list):
                        axes = "".join(
                            axis.get("name", "")
                            if isinstance(axis, dict)
                            else str(axis)
                            for axis in axes_value
                        )
                    elif isinstance(axes_value, str):
                        axes = axes_value
                    break
        return {"axes": axes} if axes else {}

    def _channel_count_from_axes(
        self,
        axes: object,
        shape: object,
    ) -> int | None:
        if not isinstance(axes, str):
            return None
        normalized_axes = axes.upper()
        if "C" not in normalized_axes:
            return None
        if not isinstance(shape, (list, tuple)):
            return None
        idx = normalized_axes.index("C")
        if idx >= len(shape):
            return None

        return int(shape[idx])

    def _coerce_v3_dtype(self, data_type: object) -> str:
        if isinstance(data_type, str):
            return data_type
        if isinstance(data_type, dict):
            name = data_type.get("name")
            if isinstance(name, str):
                return name
        return str(data_type)

    def _coerce_v3_chunk_shape(self, chunk_grid: object) -> list[int] | None:
        if not isinstance(chunk_grid, dict):
            return None
        configuration = chunk_grid.get("configuration")
        if not isinstance(configuration, dict):
            return None
        chunk_shape = configuration.get("chunk_shape")
        if not isinstance(chunk_shape, list):
            return None
        return [int(value) for value in chunk_shape]
