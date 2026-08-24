# Security model

Sky DataFlow treats input records and JSONL files as untrusted data.

## Controls

- input rows are bounded by a configurable maximum
- malformed JSON fails with a precise line number
- records must be JSON objects
- normalized column-name collisions fail closed
- required/deduplication columns are validated before processing
- output and manifest files use atomic replacement
- the container runs as an unprivileged user
- CI compiles, lints, tests, audits dependencies, and builds the image

## Limitations

SHA-256 run digests are integrity identifiers, not signatures. The product does not protect against an attacker who can rewrite both outputs and manifests, does not encrypt data at rest, and does not sandbox arbitrary custom transform functions. Only trusted code should be supplied as a transform.

Operators are responsible for filesystem permissions, sensitive-data handling, backup, secrets, retention, and network/storage controls around production datasets.
