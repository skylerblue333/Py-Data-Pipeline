# Sky DataFlow

Sky DataFlow is a deterministic batch ETL component for the **SKYCOIN4444** engineering ecosystem. It builds on Pandas and Prefect rather than reimplementing dataframe or orchestration infrastructure.

## Verified product scope

- deterministic column normalization with collision detection
- required-column validation
- bounded row-count policies
- null-row filtering and key-based deduplication
- composable copy-on-transform DataFrame steps
- stable SHA-256 input/output digests
- JSONL ingestion with line-numbered malformed-input failures
- atomic JSONL output replacement
- atomic run-manifest persistence
- safe replay detection when input digest and policy are unchanged
- Prefect-managed in-memory flow for programmatic composition
- Python 3.11/3.12 verification
- Ruff, pytest, `pip-audit`, Docker build, and non-root CI gates
- non-root container with `/data` persistence boundary

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[test]'
pytest
```

### Programmatic use

```python
from pipeline import PipelinePolicy, process_records

records = [
    {"Customer ID": 1, "Amount": 10},
    {"Customer ID": 1, "Amount": 20},
]
policy = PipelinePolicy(
    required_columns=("customer_id", "amount"),
    dedupe_keys=("customer_id",),
)
result = process_records(records, policy)
print(result.rows_in, result.rows_out, result.output_digest)
```

### Durable JSONL job

```python
from pipeline import PipelinePolicy, run_jsonl_job

result = run_jsonl_job(
    "input.jsonl",
    "output.jsonl",
    "run-manifest.json",
    PipelinePolicy(required_columns=("id",), dedupe_keys=("id",)),
)
print(result.status)
```

A second invocation with the same normalized input and policy returns `replayed` when the prior output and manifest still exist.

## Integrity model

Digests and manifests provide deterministic run evidence and accidental-change detection. They are **not digital signatures** and do not make an untrusted filesystem tamper-proof. Atomic replacement prevents readers from observing a partially written output file on a normal local filesystem.

## Product boundary

This release is a focused **single-process batch ETL engine**. It does not claim Spark-scale distributed compute, Airflow/Prefect-server HA, transactional database sinks, exactly-once distributed delivery, object-store connectors, streaming semantics, schema-registry compatibility, or regulatory certification.

Production operators remain responsible for input trust boundaries, filesystem permissions, storage durability, backup, secrets, observability, and deployment architecture.

## Container

```bash
docker build -t sky-dataflow .
docker run --rm sky-dataflow
```

The image runs as an unprivileged user. Mount `/data` when using file-based jobs.

## Open-source foundation

Pandas supplies tabular transformations and Prefect supplies orchestration primitives. Downstream distributions must preserve applicable third-party licenses and notices.

## Repository role

Sky DataFlow is product #12 in the standalone-product master plan. It remains independently testable and deployable while providing a clean future data boundary for Sky Analytics, HopeAI, commerce, finance, and reporting applications.
