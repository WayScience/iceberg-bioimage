"""Command-line interface for iceberg_bioimage."""

from __future__ import annotations

import argparse
import json

from iceberg_bioimage.api import register_store, scan_store
from iceberg_bioimage.models.scan_result import (
    ContractValidationResult,
    ScanResult,
)
from iceberg_bioimage.publishing.chunk_index import publish_chunk_index
from iceberg_bioimage.validation.contracts import validate_microscopy_profile_table


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""

    parser = argparse.ArgumentParser(prog="iceberg-bioimage")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_parser = subparsers.add_parser(
        "scan",
        help="Scan a dataset and print a summary.",
    )
    scan_parser.add_argument("uri")
    scan_parser.add_argument(
        "--json",
        action="store_true",
        help="Print the full serialized ScanResult instead of a summary.",
    )
    scan_parser.set_defaults(handler=_handle_scan)

    register_parser = subparsers.add_parser(
        "register",
        help="Publish scan metadata into an Iceberg image_assets table.",
    )
    register_parser.add_argument("uri")
    register_parser.add_argument("--catalog", required=True)
    register_parser.add_argument("--namespace", required=True)
    register_parser.add_argument("--table-name", default="image_assets")
    register_parser.add_argument(
        "--publish-chunks",
        action="store_true",
        help="Also publish derived chunk metadata to the chunk_index table.",
    )
    register_parser.add_argument("--chunk-table-name", default="chunk_index")
    register_parser.set_defaults(handler=_handle_register)

    validate_parser = subparsers.add_parser(
        "validate-contract",
        help="Validate a profile table against the microscopy join contract.",
    )
    validate_parser.add_argument("profile_table")
    validate_parser.add_argument(
        "--json",
        action="store_true",
        help="Print the full validation result as JSON.",
    )
    validate_parser.set_defaults(handler=_handle_validate_contract)

    chunk_parser = subparsers.add_parser(
        "publish-chunks",
        help="Publish derived chunk metadata into an Iceberg chunk_index table.",
    )
    chunk_parser.add_argument("uri")
    chunk_parser.add_argument("--catalog", required=True)
    chunk_parser.add_argument("--namespace", required=True)
    chunk_parser.add_argument("--table-name", default="chunk_index")
    chunk_parser.set_defaults(handler=_handle_publish_chunks)

    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CLI."""

    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.handler(args))


def _handle_scan(args: argparse.Namespace) -> int:
    scan_result = scan_store(args.uri)

    if args.json:
        print(scan_result.to_json(indent=2, sort_keys=True))
        return 0

    print(_scan_summary(scan_result))
    return 0


def _handle_register(args: argparse.Namespace) -> int:
    registration = register_store(
        args.uri,
        args.catalog,
        args.namespace,
        image_assets_table=args.table_name,
        chunk_index_table=(
            args.chunk_table_name if args.publish_chunks else None
        ),
    )
    payload = {
        "catalog": args.catalog,
        "namespace": args.namespace,
        "image_assets_table": args.table_name,
        "image_assets_rows_published": registration.image_assets_rows_published,
        "chunk_rows_published": registration.chunk_rows_published,
        "source_uri": args.uri,
    }
    if args.publish_chunks:
        payload["chunk_table_name"] = args.chunk_table_name

    print(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _handle_validate_contract(args: argparse.Namespace) -> int:
    result = validate_microscopy_profile_table(args.profile_table)

    if args.json:
        print(result.to_json(indent=2, sort_keys=True))
    else:
        print(_contract_summary(result))

    return 0 if result.is_valid else 1


def _handle_publish_chunks(args: argparse.Namespace) -> int:
    scan_result = scan_store(args.uri)
    row_count = publish_chunk_index(
        catalog=args.catalog,
        namespace=args.namespace,
        table_name=args.table_name,
        scan_result=scan_result,
    )
    print(
        json.dumps(
            {
                "catalog": args.catalog,
                "namespace": args.namespace,
                "table_name": args.table_name,
                "rows_published": row_count,
                "source_uri": args.uri,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _scan_summary(scan_result: ScanResult) -> str:
    lines = [
        f"source_uri: {scan_result.source_uri}",
        f"format_family: {scan_result.format_family}",
        f"image_assets: {len(scan_result.image_assets)}",
    ]

    for asset in scan_result.image_assets:
        label = asset.array_path or "<root>"
        lines.append(
            f"- {label}: shape={asset.shape} dtype={asset.dtype}"
            + (f" chunks={asset.chunk_shape}" if asset.chunk_shape else "")
        )

    if scan_result.warnings:
        lines.append("warnings:")
        lines.extend(f"- {warning}" for warning in scan_result.warnings)

    return "\n".join(lines)


def _contract_summary(result: ContractValidationResult) -> str:
    lines = [
        f"target: {result.target}",
        f"is_valid: {result.is_valid}",
        f"required_columns: {', '.join(result.required_columns)}",
        f"recommended_columns: {', '.join(result.recommended_columns)}",
    ]

    if result.missing_required_columns:
        lines.append(
            "missing_required_columns: "
            + ", ".join(result.missing_required_columns)
        )

    if result.missing_recommended_columns:
        lines.append(
            "missing_recommended_columns: "
            + ", ".join(result.missing_recommended_columns)
        )

    if result.warnings:
        lines.append("warnings:")
        lines.extend(f"- {warning}" for warning in result.warnings)

    return "\n".join(lines)


if __name__ == "__main__":
    raise SystemExit(main())
