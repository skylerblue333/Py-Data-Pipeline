from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

COLUMN_RE = re.compile(r"[^a-z0-9_]+")


@dataclass(frozen=True)
class PipelinePolicy:
    required_columns: tuple[str, ...] = ()
    drop_null_required: bool = True
    deduplicate: bool = True
    max_rows: int = 100_000

    def __post_init__(self) -> None:
        if self.max_rows < 1 or self.max_rows > 10_000_000:
            raise ValueError("max_rows must be between 1 and 10,000,000")


@dataclass(frozen=True)
class PipelineResult:
    records: list[dict[str, Any]]
    rows_in: int
    rows_out: int
    rows_dropped: int
    input_digest: str
    output_digest: str


def normalize_column(value: str) -> str:
    normalized = COLUMN_RE.sub("_", value.strip().lower()).strip("_")
    if not normalized:
        raise ValueError(f"column name {value!r} normalizes to an empty identifier")
    return normalized


def normalize_schema(columns: Iterable[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    destinations: dict[str, str] = {}
    for original in columns:
        normalized = normalize_column(str(original))
        if normalized in destinations and destinations[normalized] != original:
            raise ValueError(
                f"schema collision: {destinations[normalized]!r} and {original!r} both normalize to {normalized!r}"
            )
        mapping[original] = normalized
        destinations[normalized] = original
    return mapping


def stable_digest(records: list[dict[str, Any]]) -> str:
    encoded = json.dumps(records, separators=(",", ":"), sort_keys=True, ensure_ascii=False, allow_nan=False, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def policy_digest(policy: PipelinePolicy) -> str:
    encoded = json.dumps(asdict(policy), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def transform(records: list[dict[str, Any]], policy: PipelinePolicy) -> PipelineResult:
    if len(records) > policy.max_rows:
        raise ValueError(f"input contains {len(records)} rows; max_rows={policy.max_rows}")
    if not records:
        digest = stable_digest([])
        return PipelineResult([], 0, 0, 0, digest, digest)

    input_digest = stable_digest(records)
    frame = pd.DataFrame.from_records(records)
    mapping = normalize_schema([str(column) for column in frame.columns])
    frame = frame.rename(columns=mapping)
    required = [normalize_column(column) for column in policy.required_columns]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"required columns missing: {', '.join(missing)}")

    if policy.drop_null_required and required:
        frame = frame.dropna(subset=required)
    if policy.deduplicate:
        frame = frame.drop_duplicates()

    output_records = json.loads(frame.to_json(orient="records", date_format="iso"))
    return PipelineResult(
        records=output_records,
        rows_in=len(records),
        rows_out=len(frame),
        rows_dropped=len(records) - len(frame),
        input_digest=input_digest,
        output_digest=stable_digest(output_records),
    )


def read_jsonl(path: str | Path, max_rows: int) -> list[dict[str, Any]]:
    source = Path(path)
    records: list[dict[str, Any]] = []
    with source.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            if len(records) >= max_rows:
                raise ValueError(f"input exceeds max_rows={max_rows}")
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON on line {line_number}") from exc
            if not isinstance(value, dict):
                raise TypeError(f"line {line_number} must contain a JSON object")
            records.append(value)
    return records


def _atomic_json_write(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, sort_keys=True, indent=2, default=str)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def run_file(input_path: str | Path, output_path: str | Path, manifest_path: str | Path, policy: PipelinePolicy) -> dict[str, Any]:
    input_records = read_jsonl(input_path, policy.max_rows)
    result = transform(input_records, policy)
    output = Path(output_path)
    manifest = Path(manifest_path)
    signature = hashlib.sha256(f"{result.input_digest}:{policy_digest(policy)}".encode("utf-8")).hexdigest()

    if manifest.exists() and output.exists():
        try:
            previous = json.loads(manifest.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            previous = {}
        if previous.get("run_signature") == signature:
            return {**previous, "replayed": True}

    _atomic_json_write(output, result.records)
    manifest_value = {
        "run_signature": signature,
        "input_digest": result.input_digest,
        "output_digest": result.output_digest,
        "policy_digest": policy_digest(policy),
        "rows_in": result.rows_in,
        "rows_out": result.rows_out,
        "rows_dropped": result.rows_dropped,
        "output": str(output),
        "replayed": False,
    }
    _atomic_json_write(manifest, manifest_value)
    return manifest_value


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a deterministic Sky DataFlow JSONL batch")
    parser.add_argument("input")
    parser.add_argument("output")
    parser.add_argument("--manifest", default="dataflow-manifest.json")
    parser.add_argument("--required", action="append", default=[])
    parser.add_argument("--max-rows", type=int, default=100_000)
    parser.add_argument("--keep-null-required", action="store_true")
    parser.add_argument("--keep-duplicates", action="store_true")
    args = parser.parse_args()
    policy = PipelinePolicy(
        required_columns=tuple(args.required),
        drop_null_required=not args.keep_null_required,
        deduplicate=not args.keep_duplicates,
        max_rows=args.max_rows,
    )
    manifest = run_file(args.input, args.output, args.manifest, policy)
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
