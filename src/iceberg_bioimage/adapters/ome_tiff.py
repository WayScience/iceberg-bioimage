"""OME-TIFF adapter."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import tifffile

from iceberg_bioimage.adapters.base import BaseAdapter
from iceberg_bioimage.models.scan_result import ImageAsset, ScanResult

_REMOTE_SCHEMES = frozenset({"s3", "gs", "az", "abfs", "http", "https"})


def _is_remote_uri(uri: str) -> bool:
    return urlparse(uri).scheme in _REMOTE_SCHEMES


class OMETiffAdapter(BaseAdapter):
    """Scan TIFF files into canonical image assets."""

    name = "ome-tiff"
    format_family = "ome-tiff"

    def can_handle(self, uri: str) -> bool:
        normalized = uri.lower()
        return normalized.endswith((".tif", ".tiff"))

    def scan(self, uri: str) -> ScanResult:
        image_assets: list[ImageAsset] = []

        with self._open_tiff(uri) as tif:
            for index, series in enumerate(tif.series):
                array_path = None if len(tif.series) == 1 else f"series/{index}"
                axes = getattr(series, "axes", "")
                metadata = {
                    "series_index": index,
                    "axes": axes,
                    "channel_count": self._channel_count(axes, series.shape),
                    "ndim": len(series.shape),
                }
                image_assets.append(
                    ImageAsset(
                        uri=uri,
                        array_path=array_path,
                        shape=[int(value) for value in series.shape],
                        dtype=str(series.dtype),
                        metadata=metadata,
                        image_id=self._image_id(uri, index),
                    )
                )

        if not image_assets:
            raise ValueError(f"No image series were discovered in TIFF file {uri!r}.")

        return ScanResult(
            source_uri=uri,
            format_family=self.format_family,
            image_assets=image_assets,
        )

    def _open_tiff(self, uri: str) -> tifffile.TiffFile:
        if _is_remote_uri(uri):
            try:
                import fsspec
            except ImportError as exc:
                raise ImportError(
                    "fsspec is required to open remote TIFF URIs. "
                    "Install it with: pip install fsspec  "
                    "(for S3 also install s3fs)."
                ) from exc
            fileobj = fsspec.open(uri, "rb").open()
            return tifffile.TiffFile(fileobj)
        return tifffile.TiffFile(uri)

    def _channel_count(self, axes: str, shape: tuple[int, ...]) -> int | None:
        if "C" not in axes:
            return None

        return int(shape[axes.index("C")])

    def _image_id(self, uri: str, index: int) -> str:
        name = Path(uri).name
        normalized = name.casefold()

        for suffix in (".ome.tiff", ".ome.tif", ".tiff", ".tif"):
            if normalized.endswith(suffix):
                stem = name[: -len(suffix)]
                break
        else:
            stem = name

        return stem if index == 0 else f"{stem}:series-{index}"
