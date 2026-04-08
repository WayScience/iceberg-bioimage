# Cytomining Warehouse Specification (RFC 2119)

## Status

Draft, normative for cytomining ecosystem implementations.

## Terminology

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**,
and **MAY** are to be interpreted as described in [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119).

## Scope

This specification defines a portable warehouse layout and naming contract for
projects in and beyond the cytomining ecosystem, including:

- `cytotable`
- `pycytominer`
- `coSMicQC`
- `ome-arrow`
- `cytodataframe`
- `iceberg-bioimage`

This specification constrains warehouse structure, interoperability behavior,
and manifest semantics. Detailed per-table column schema constraints are
expected to be defined in a companion schema specification.

The structural core is bioimaging-general. Cytomining-specific table
conventions in this document can be treated as a profile layered on that core.

## Specification Versioning

1. Every warehouse manifest **MUST** declare a `warehouse_spec_version`.
1. Spec versions **MUST** use semantic versioning (`MAJOR.MINOR.PATCH`).
1. A `MAJOR` version change **MUST** indicate a breaking interoperability
   change.
1. A `MINOR` version change **MUST** be backward-compatible for conforming
   readers.
1. A `PATCH` version change **MUST NOT** change interoperability requirements;
   it is limited to clarifications, errata, and non-normative fixes.
1. Readers **SHOULD** fail fast with a clear error when they cannot interpret a
   declared major version.

## Conformance Profiles

1. Implementations **MUST** declare at least one supported conformance profile.
1. The following profiles are defined:
   - `core`: namespaced layout + manifest conformance
   - `profiles-qc`: core + multi-grain profile and QC interoperability
   - `images-crops`: core + image asset and crop/source-image interoperability
   - `full`: all requirements in this specification
1. Producers **MUST NOT** claim a profile unless all requirements for that
   profile are met.
1. Consumers **SHOULD** publish the profiles they can read without degraded
   behavior.

## Core Model

1. A warehouse root **MUST** organize tables by Iceberg-style namespace and
   table identifier.
1. A canonical identifier **MUST** be represented as `<namespace>.<table>`.
1. On local filesystems, `<namespace>.<table>` **MUST** map to
   `<warehouse_root>/<namespace>/<table>/`.
1. Warehouse producers **MUST NOT** treat flat top-level table folders as the
   canonical layout for new writes.

## Canonical Namespaces

1. Profile-oriented tables **MUST** use the `profiles` namespace.
1. Image-metadata and image-derived indexing tables **MUST** use the `images`
   namespace.
1. Implementations **SHOULD** reject ambiguous writes that omit namespace when
   multiple namespace defaults are possible.

## Image Source Conventions

1. Image assets discovered from OME-Zarr, OME-TIFF, and TIFF sources **MUST**
   be represented under the `images` namespace using canonical image metadata
   tables.
1. Source format differences (for example chunked OME-Zarr vs non-chunked TIFF)
   **MUST NOT** change namespace placement.
1. Canonical image metadata for all supported source formats **MUST** be stored
   in `images.image_assets`.
1. Chunk-derived metadata **MAY** be stored in `images.chunk_index`; producers
   **MUST** write zero rows (or omit the table) when source assets do not
   expose chunk metadata.
1. Image identifiers **MUST** remain stable across source format variants so
   downstream joins to `profiles.*` and QC tables are format-agnostic.
1. Format-specific metadata fields **MAY** be included, but producers
   **SHOULD** preserve common canonical fields (`dataset_id`, `image_id`,
   shape/dtype metadata) for interoperability.

## Canonical Tables

1. `profiles.joined_profiles` **MUST** be the canonical joined profile table
   name when a joined profile table is present.
1. `images.image_crops` **MUST** be used for per-object image crops when such a
   table is produced.
1. `images.source_images` **MUST** be used for source-image payload tables when
   such a table is produced.
1. `profiles.profile_with_images` **MAY** be produced as a derived analytical
   view when both profile and crop tables are present.
1. Additional project-specific tables **MAY** exist, but **SHOULD** remain in
   a namespace consistent with their semantics (typically `profiles` or
   `images`).

## Role Vocabulary

1. Manifest table roles **MUST** come from a controlled vocabulary.
1. Standard roles **MUST** include at least:
   - `image_assets`
   - `chunk_index`
   - `joined_profiles`
   - `quality_control`
1. Standard optional roles **MAY** include:
   - `image_crops`
   - `source_images`
   - `embeddings`
   - `annotations`
   - `reports`
1. Project-specific roles **MAY** be added, but producers **MUST** document
   their semantics.

## Quality Control Tables

1. QC datasets that are intended to filter profiles (for example coSMicQC
   outputs) **MUST** be stored as profile-domain tables, typically under
   `profiles.<qc_table_name>`.
1. A QC table used for profile filtering **MUST** include `dataset_id` and
   `image_id`.
1. A QC table used for object-level filtering **SHOULD** include `object_id`
   when available.
1. QC filtering state **MUST** be representable as a boolean column. Producers
   **SHOULD** use canonical `qc_pass`; readers **SHOULD** accept `QC_Pass` for
   compatibility.
1. When a QC table is exported, its manifest `role` **MUST** identify the
   table as quality-control data (for example `quality_control` or a
   project-specific QC role).
1. Consumers that apply QC filters to profiles **MUST** join by the available
   canonical keys and **MUST NOT** silently change join grain (image-level vs
   object-level).

## Multi-Dimensional Profile Data

1. Warehouses **MAY** contain multiple profile tables at different biological
   or analytical grains, including organoid-level, image-level, and
   object/compartment-level tables.
1. Multi-grain profile tables **MUST** be stored as distinct tables (for
   example `profiles.organoid_profiles`, `profiles.joined_profiles`,
   `profiles.nuclei_profiles`) and **MUST NOT** rely on implicit mixed-grain
   rows in one table.
1. Every profile table, regardless of grain, **MUST** include `dataset_id` and
   `image_id`.
1. Object/compartment-level profile tables **MUST** include `object_id` (or a
   normalized alias mapped to `object_id`) for stable row identity.
1. Profile tables **SHOULD** include explicit grain metadata, such as
   `profile_level` (for example `organoid`, `image`, `object`) and, when
   relevant, `compartment` (for example `cells`, `nuclei`, `cytoplasm`).
1. Producers **MUST** document join grain in manifest metadata via `role` and
   **SHOULD** include grain-indicating table names.
1. Consumers joining or filtering across grains **MUST** use grain-compatible
   keys and **MUST NOT** assume one-to-one cardinality between different
   profile levels.

## Grain and Cardinality Contract

1. Producers **MUST** declare the intended grain for each profile and QC table
   in manifest metadata.
1. Consumers **MUST** validate grain compatibility before executing joins or
   filters across tables.
1. If a requested operation implies an unsafe grain expansion or collapse,
   consumers **MUST** require explicit user intent and **MUST NOT** proceed
   silently.
1. Filtering behavior **MUST** preserve row provenance so downstream tools can
   audit which rows were retained or removed.

## Join Keys

1. Tables intended for cross-project joins **MUST** include `dataset_id` and
   `image_id`.
1. Producers **SHOULD** also preserve `plate_id`, `well_id`, and `site_id`
   when available.
1. Profile-column alias normalization **MAY** be used at write time, but
   canonical output column names **MUST** use the normalized keys above.

## Manifest

1. Warehouse roots **MUST** include `warehouse_manifest.json`.
1. Manifest entries **MUST** record table identifier as the canonical dotted
   name (`<namespace>.<table>`).
1. Manifest entries **MUST** include:
   - `table_name`
   - `role`
   - `format`
   - `join_keys`
   - `columns`
1. Manifest table names **MUST** be unique within one manifest.
1. Manifest metadata **MUST** include enough information for a reader to
   determine role, grain, and conformance profile compatibility without
   inspecting table contents.

## Loader and Resolution Behavior

1. Loaders that auto-resolve a target table **MUST** do so only when resolution
   is unambiguous.
1. When multiple compatible targets exist, loaders **MUST** require explicit
   table selection.
1. Error messages for ambiguous resolution **SHOULD** include candidate table
   identifiers and selection guidance.

## Consumer Interoperability

1. Profile-processing tools (for example `pycytominer`) **SHOULD** support
   explicit table selection by canonical identifier.
1. Table/visualization tools (for example `cytodataframe`) **SHOULD** honor
   manifest grain and role metadata to prevent invalid cross-grain displays.
1. QC-aware consumers **MUST** apply QC semantics according to declared QC
   scope and grain.

## Compatibility

1. Readers **SHOULD** support legacy flat layouts for backward compatibility.
1. Writers targeting this specification **MUST** emit namespaced layout by
   default.
1. Projects migrating from flat layouts **SHOULD** preserve stable table roles
   and join semantics while moving paths/names to the canonical form.

## Independent and External Writes

1. Data **MAY** be added to an existing warehouse by tools other than this
   package.
1. External writers **MUST** preserve canonical table identifiers,
   namespace-path mapping, and manifest requirements defined by this
   specification.
1. External writers **MUST** update `warehouse_manifest.json` to reflect added,
   replaced, or removed tables before declaring the warehouse ready for shared
   consumption.
1. External writes **SHOULD** be staged atomically (or as close as practical)
   so readers do not observe partially updated table/manifest state.
1. Appending data to an existing table **MUST NOT** silently change declared
   role or grain semantics for that table.
1. Replacing a table in-place **MUST** preserve identifier stability
   (`<namespace>.<table>`) and **SHOULD** preserve role semantics unless a
   documented migration is performed.
1. If an external write changes grain, role, or join behavior, the writer
   **MUST** treat it as a compatibility-affecting change and **SHOULD**
   communicate required consumer updates.
1. External writers **SHOULD** record provenance in manifest metadata (for
   example source workflow or producer identity) so downstream tools can audit
   table origin.
1. After external modification, maintainers **SHOULD** run conformance
   validation before distributing or relying on the updated warehouse.
1. Consumers **MUST NOT** assume that all tables were produced by one package;
   they **MUST** rely on manifest semantics and conformance rules instead.

## Conformance

An implementation conforms to this specification if it satisfies all
requirements marked **MUST** and **MUST NOT**.

## Conformance Test Suite

1. The ecosystem **SHOULD** maintain a shared conformance fixture set with
   representative warehouses for each conformance profile.
1. Projects claiming conformance **SHOULD** run the shared conformance suite in
   CI.
1. Conformance claims **SHOULD** identify the tested specification version and
   supported profiles.

## Quick Start by Role

1. Producer:
   write namespaced tables, record role/grain in the manifest, validate before
   sharing.
1. Consumer:
   read by canonical identifier, enforce grain compatibility, fail on
   ambiguous resolution.
1. QC maintainer:
   publish QC tables in `profiles.*`, declare QC semantics in manifest role and
   scope metadata, verify filter joins.
1. External integrator:
   preserve identifiers and role/grain semantics, update manifest atomically,
   rerun conformance validation.

## Common Failure Modes

1. Ambiguous auto-resolution:
   provide explicit `<namespace>.<table>`.
1. Grain mismatch in joins or filters:
   align grains or require explicit expansion/collapse intent.
1. Manifest drift after external writes:
   regenerate manifest metadata and rerun conformance checks.
1. Unsupported specification major version:
   stop read/write path and require compatible tooling.
