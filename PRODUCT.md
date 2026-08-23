# Sky DataFlow product scope

**Product:** Sky DataFlow

**Purpose:** deterministic, restart-aware batch ETL for local or containerized data-processing jobs.

**Core value:** normalize tabular records, enforce simple schema/quality policies, write atomic JSONL outputs, and persist digest-backed run manifests that make repeated runs observable and replay-safe when inputs are unchanged.

**Intended integrations:** Sky Analytics, HopeAI ingestion, commerce/reporting exports, and other SKYCOIN4444 services that need a small batch-processing boundary.

**Not included:** distributed compute, stream processing, managed orchestration, cloud object-store/database connectors, distributed exactly-once guarantees, or compliance certification.
