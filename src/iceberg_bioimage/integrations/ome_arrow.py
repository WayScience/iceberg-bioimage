"""Optional OME-Arrow integration helpers (requires ome-arrow >= 0.0.10)."""

from __future__ import annotations

import importlib
from typing import Any


def create_ome_arrow(data: Any, **kwargs: Any) -> object:  # noqa: ANN401
    """Create an ``ome_arrow.OMEArrow`` object when the optional extra is installed."""

    ome_arrow = _require_ome_arrow()
    return ome_arrow.OMEArrow(data=data, **kwargs)


def scan_ome_arrow(data: str, **kwargs: Any) -> object:  # noqa: ANN401
    """Create a lazy ``ome_arrow.OMEArrow`` scan plan for tabular image sources."""

    ome_arrow = _require_ome_arrow()
    return ome_arrow.OMEArrow.scan(data=data, **kwargs)


def create_ome_arrow_from_zarr(zarr_path: str, **kwargs: Any) -> object:  # noqa: ANN401
    """Read an OME-Zarr store and return a typed OME-Arrow ``pa.StructScalar``.

    Requires ``bioio`` and an OME-Zarr reader backend in the environment.
    Any keyword arguments are forwarded to ``ome_arrow.from_ome_zarr``.

    Conversion cost: ``ome_arrow.from_ome_zarr`` eagerly reads every plane of
    the array into memory and re-encodes it into Arrow; there is no lazy or
    streaming path. Expect cost roughly proportional to reading the full
    pixel array once, plus Arrow encoding overhead. For repeated reads of the
    same data, convert once and reuse :func:`open_ome_arrow_dataset` instead
    of calling this function each time.
    """

    ome_arrow = _require_ome_arrow()
    return ome_arrow.from_ome_zarr(zarr_path, **kwargs)


def create_ome_arrow_from_tiff(tiff_path: str, **kwargs: Any) -> object:  # noqa: ANN401
    """Read a TIFF file and return a typed OME-Arrow ``pa.StructScalar``.

    Requires ``bioio`` in the environment.
    Any keyword arguments are forwarded to ``ome_arrow.from_tiff``.

    Conversion cost: same as :func:`create_ome_arrow_from_zarr` — this reads
    and re-encodes the full pixel array eagerly, with no lazy path.
    """

    ome_arrow = _require_ome_arrow()
    return ome_arrow.from_tiff(tiff_path, **kwargs)


def open_ome_arrow_dataset(path: str) -> object:
    """Open an OME-Arrow dataset directory for reading.

    Returns an ``ome_arrow.OMEArrowDataset`` instance that exposes
    ``read_image``, ``read_channel``, ``read_plane``, ``read_region``,
    and ``image_metadata`` methods.
    """

    ome_arrow = _require_ome_arrow()
    return ome_arrow.OMEArrowDataset(path)


def write_ome_arrow_dataset(
    images: Any,  # noqa: ANN401
    output_path: str,
    **kwargs: Any,  # noqa: ANN401
) -> object:
    """Write a sequence of images as an OME-Arrow dataset on disk.

    *images* may be NumPy arrays, OME-Arrow records, or any input accepted
    by ``ome_arrow.write_ome_arrow_dataset``.  Any keyword arguments are
    forwarded verbatim.

    Returns the ``ChunkChoice`` used for the first image.
    """

    ome_arrow = _require_ome_arrow()
    return ome_arrow.write_ome_arrow_dataset(images, output_path, **kwargs)


def _require_ome_arrow() -> object:
    try:
        ome_arrow = importlib.import_module("ome_arrow")
    except ImportError as exc:  # pragma: no cover - exercised without extra
        raise RuntimeError(
            "OME-Arrow helpers require the optional ome-arrow extra. "
            "Install it with `pip install 'iceberg-bioimage[ome-arrow]'` "
            "or `uv sync --group ome-arrow`."
        ) from exc

    return ome_arrow
