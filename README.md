# Py Data Pipeline

Reusable Python ETL/data-processing component for the **SKYCOIN4444** ecosystem. The implementation deliberately builds on established open-source primitives—**Pandas** for tabular processing and **Prefect** for orchestration—instead of recreating commodity infrastructure.

## Current execution surface

- Prefect-managed `skycoin-data-pipeline` flow
- Record loading from Python dictionaries into Pandas DataFrames
- deterministic column normalization
- composable DataFrame transform chain
- null-row filtering
- copy-on-transform behavior to avoid accidental input mutation
- row-count accounting
- runtime validation that transforms return DataFrames
- executable pytest coverage for core transforms
- reproducible Python project metadata
- GitHub Actions CI on Python 3.11 and 3.12
- minimal Docker runtime

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pytest
python pipeline.py
```

Windows PowerShell activation:

```powershell
.venv\Scripts\Activate.ps1
```

## Example

```python
from pipeline import run_pipeline

records = [{" Customer Name ": "Sky", "Amount": 10}]
result = run_pipeline(records)
print(result)
```

The deterministic normalization step produces keys such as `customer_name` and `amount`.

## Verification

CI performs dependency installation, Python compilation, and the pytest suite on every push and pull request. The repository does **not** claim production readiness merely because CI exists.

Current verified scope is the in-memory ETL foundation. Production connectors, durable storage, schema contracts, lineage, distributed scheduling, observability, secrets management, and deployed-environment evidence remain separate work.

## Ecosystem boundary

**SKYCOIN4444 → Data / Analytics → downstream AI, reporting, market-data, and platform services**

The component is intentionally small and reusable so it can later be composed into a larger workspace without pretending that unimplemented infrastructure already exists.

## Environment

See `.env.example`. The current pipeline does not require credentials or external services.

## Container

```bash
docker build -t skycoin-data-pipeline .
docker run --rm skycoin-data-pipeline
```

## Open-source foundation

Pandas and Prefect provide the commodity dataframe and orchestration layers. Their licenses and all other third-party dependency notices must be preserved in downstream distributions.

## Production checklist

- [ ] durable database/object-store connector
- [ ] schema validation and versioning
- [ ] retry and dead-letter semantics
- [ ] incremental/idempotent loading
- [ ] data-quality assertions
- [ ] OpenTelemetry traces/metrics
- [ ] production secrets integration
- [ ] integration/load tests against real backends
- [ ] deployed staging environment

## License

See the checked-in repository license and applicable third-party dependency licenses.
