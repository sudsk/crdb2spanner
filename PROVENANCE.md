# Provenance and clean-room record

## Policy

1. `storj/spanner-migration` is licensed **AGPL-3.0**. Contributors to this repo **must not** read, copy, translate or paraphrase its source code. That includes feeding it to AI coding assistants used on this repo.
2. Using its *published README* as a statement of the problem is acceptable: ideas and architecture are not copyrightable. Its code is.
3. Design inputs are limited to public vendor documentation and permissively licensed (Apache-2.0 / BSD / MIT) projects. Each one is listed below.
4. Reusing Apache-2.0 code (e.g. from Google DataflowTemplates) is allowed. Keep the original licence header and add an entry to `NOTICE`.
5. Any contributor who has studied the Storj code may act only as a *spec author*: they write behaviour descriptions, not code.

## Design sources

| Source | Licence | Used for |
|---|---|---|
| CockroachDB docs: changefeeds, changefeed messages, `EXPORT`, `AS OF SYSTEM TIME`, protected timestamps | Docs | CDC and snapshot semantics |
| Google Cloud docs: Migrate to Firestore with MongoDB compatibility | CC-BY-4.0 | Operator-guide structure |
| Google Cloud docs: Spanner migration tools, SMT, PostgreSQL → Spanner minimal-downtime blog | CC-BY-4.0 | Phase model, SMT reuse |
| GoogleCloudPlatform/spanner-migration-tool | Apache-2.0 *(verify LICENSE file)* | Schema conversion, session file |
| GoogleCloudPlatform/DataflowTemplates (sourcedb-to-spanner, datastream-to-spanner, spanner-to-sourcedb) | Apache-2.0 | Design patterns (shadow tables, DLQ), optional bulk path |
| Apache Beam Python SDK | Apache-2.0 | Pipelines |
| GoogleCloudPlatform/professional-services-data-validator (DVT) | Apache-2.0 | Validation |
| PGAdapter | Apache-2.0 | Application connectivity |

## Log

| Date | Who | Note |
|---|---|---|
| 2026-10-25 | Suds / Claude | Initial README and ARCHITECTURE written from vendor docs only |
| 2026-10-25 | Suds / Claude | v0.1 code scaffold written from vendor docs + Apache-2.0 Beam/Spanner client APIs only |
