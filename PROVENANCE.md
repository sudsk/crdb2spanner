# Provenance and clean-room record

## Policy

1. Design inputs are limited to public vendor documentation and permissively licensed (Apache-2.0 / BSD / MIT) projects. Each one is listed below.
2. Do not copy, translate or paraphrase source code from copyleft-licensed (GPL / AGPL) projects into this repo. That includes feeding such code to AI coding assistants used on this repo.
3. Reusing Apache-2.0 code (e.g. from Google DataflowTemplates) is allowed. Keep the original licence header and add an entry to `NOTICE`.

## Design sources

| Source | Licence | Used for |
|---|---|---|
| CockroachDB docs: changefeeds, changefeed messages, `EXPORT`, `AS OF SYSTEM TIME`, protected timestamps | Docs | CDC and snapshot semantics |
| Google Cloud docs: Spanner migration tools, SMT, PostgreSQL → Spanner minimal-downtime blog | CC-BY-4.0 | Phase model, SMT reuse |
| GoogleCloudPlatform/spanner-migration-tool | Apache-2.0 *(verify LICENSE file)* | Schema conversion, session file |
| GoogleCloudPlatform/DataflowTemplates (sourcedb-to-spanner, datastream-to-spanner, spanner-to-sourcedb) | Apache-2.0 | Design patterns (shadow tables, DLQ), optional bulk path |
| Apache Beam Python SDK | Apache-2.0 | Pipelines |
| GoogleCloudPlatform/professional-services-data-validator (DVT) | Apache-2.0 | Validation |
| PGAdapter | Apache-2.0 | Application connectivity |

## Log

| Date | Who | Note |
|---|---|---|
| 2026-10-25 | Suds | Initial README and ARCHITECTURE written from vendor docs only |
| 2026-10-25 | Suds | v0.1 code scaffold written from vendor docs + Apache-2.0 Beam/Spanner client APIs only |
