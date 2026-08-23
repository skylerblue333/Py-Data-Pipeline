# Ecosystem Integration

**Role:** data transformation/orchestration service.

**Foundation:** Prefect + pandas. Use established orchestration primitives rather than inventing scheduling/retry infrastructure.

**Consumes:** typed records from platform services.

**Provides:** normalized datasets and pipeline execution state.

**Production requirements:** idempotency, schema validation, retries, observability, secrets isolation, and a repeatable deployment configuration.
