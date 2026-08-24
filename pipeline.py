from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from prefect import task

COLUMN_RE = re.compile(r"[^a-z0-9_]+")


@dataclass(frozen=True)
class PipelinePolicy:
    required_columns: tuple[str, ...] = ()
    drop_null_columns: tuple[str, ...] = ()
    dedupe_keys: tuple[str, ...] = ()
    max_rows: int = 100_000

    def __post_init__(self) -> None:
        if self.max_rows < 1 or self.max_rows > 10_000_000:
            raise ValueError("max_rows must be between 1 and 10,000,000")


@dataclass(frozen=True)
class PipelineResult:
    frame: pd.DataFrame
    rows_in: int
    rows_out: int
    rows_dropped: int
    input_digest: str
    output_digest: str


@dataclass(frozen=True)
class JobResult:
    status: str
    rows_in: int
    rows_out: int
    rows_dropped: int
    input_digest: str
    output_digest: str
    run_signature: str


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
                f"schema collision creates duplicate names: {destinations[normalized]!r} and {original!r} -> {normalized!r}"
            )
        mapping[str(original)] = normalized
        destinations[normalized] = str(original)
    return mapping


@task
def normalize(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy(deep=True)
    result = result.rename(columns=normalize_schema([str(column) for column in result.columns]))
    return result


def drop_null_rows(df: pd.DataFrame, columns: list[str] | tuple[str, ...] | None = None) -> pd.DataFrame:
    return df.dropna(subset=list(columns) if columns else None).reset_index(drop=True)


def run_transforms(
    df: pd.DataFrame,
    transforms: list[Callable[[pd.DataFrame], pd.DataFrame]] | None = None,
) -> pd.DataFrame:
    current = df.copy(deep=True)
    for transform in transforms or []:
        transformed = transform(current.copy(deep=True))
        if not isinstance(transformed, pd.DataFrame):
            raise TypeError("pipeline transforms must return pandas.DataFrame")
        current = transformed
    return current


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def stable_digest(records: list[dict[str, Any]]) -> str:
    encoded = json.dumps(
        records,
        separators=(",", ":"),
        sort_keys=True,
        ensure_ascii=False,
        allow_nan=False,
        default=str,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def policy_digest(policy: PipelinePolicy) -> str:
    encoded = json.dumps(asdict(policy), sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def process_records(records: list[dict[str, Any]], policy: PipelinePolicy | None = None) -> PipelineResult:
    effective = policy or PipelinePolicy()
    if len(records) > effective.max_rows:
        raise ValueError(f"input contains {len(records)} rows; max_rows={effective.max_rows}")

    input_digest = stable_digest(records)
    frame = normalize.fn(pd.DataFrame.from_records(records))
    required = tuple(normalize_column(column) for column in effective.required_columns)
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"required columns missing: {', '.join(missing)}")

    null_columns = tuple(normalize_column(column) for column in effective.drop_null_columns)
    missing_null = [column for column in null_columns if column not in frame.columns]
    if missing_null:
        raise ValueError(f"drop-null columns missing: {', '.join(missing_null)}")
    if null_columns:
        frame = drop_null_rows(frame, null_columns)

    dedupe_keys = tuple(normalize_column(column) for column in effective.dedupe_keys)
    missing_dedupe = [column for column in dedupe_keys if column not in frame.columns]
    if missing_dedupe:
        raise ValueError(f"dedupe columns missing: {', '.join(missing_dedupe)}")
    if dedupe_keys:
        frame = frame.drop_duplicates(subset=list(dedupe_keys), keep="first").reset_index(drop=True)

    output_records = _records(frame)
    return PipelineResult(
        frame=frame,
        rows_in=len(records),
        rows_out=len(frame),
        rows_dropped=len(records) - len(frame),
        input_digest=input_digest,
        output_digest=stable_digest(output_records),
    )


def read_jsonl(path: str | Path, max_rows: int) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as handle:
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


def _atomic_text_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def run_jsonl_job(
    input_path: str | Path,
    output_path: str | Path,
    manifest_path: str | Path,
    policy: PipelinePolicy | None = None,
) -> JobResult:
    effective = policy or PipelinePolicy()
    result = process_records(read_jsonl(input_path, effective.max_rows), effective)
    signature = hashlib.sha256(f"{result.input_digest}:{policy_digest(effective)}".encode()).hexdigest()
    output = Path(output_path)
    manifest = Path(manifest_path)

    if manifest.exists() and output.exists():
        try:
            previous = json.loads(manifest.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            previous = {}
        if previous.get("run_signature") == signature and previous.get("output_digest") == result.output_digest:
            return JobResult(
                status="replayed",
                rows_in=result.rows_in,
                rows_out=result.rows_out,
                rows_dropped=result.rows_dropped,
                input_digest=result.input_digest,
                output_digest=result.output_digest,
                run_signature=signature,
            )

    lines = "".join(json.dumps(record, sort_keys=True, ensure_ascii=False, default=str) + "\n" for record in _records(result.frame))
    _atomic_text_write(output, lines)
    metadata = {
        "status": "completed",
        "run_signature": signature,
        "input_digest": result.input_digest,
        "output_digest": result.output_digest,
        "policy_digest": policy_digest(effective),
        "rows_in": result.rows_in,
        "rows_out": result.rows_out,
        "rows_dropped": result.rows_dropped,
        "output": str(output),
    }
    _atomic_text_write(manifest, json.dumps(metadata, sort_keys=True, indent=2))
    return JobResult(
        status="completed",
        rows_in=result.rows_in,
        rows_out=result.rows_out,
        rows_dropped=result.rows_dropped,
        input_digest=result.input_digest,
        output_digest=result.output_digest,
        run_signature=signature,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a deterministic Sky DataFlow JSONL batch")
    parser.add_argument("input")
    parser.add_argument("output")
    parser.add_argument("--manifest", default="dataflow-manifest.json")
    parser.add_argument("--required", action="append", default=[])
    parser.add_argument("--drop-null", action="append", default=[])
    parser.add_argument("--dedupe-key", action="append", default=[])
    parser.add_argument("--max-rows", type=int, default=100_000)
    args = parser.parse_args()
    result = run_jsonl_job(
        args.input,
        args.output,
        args.manifest,
        PipelinePolicy(
            required_columns=tuple(args.required),
            drop_null_columns=tuple(args.drop_null),
            dedupe_keys=tuple(args.dedupe_key),
            max_rows=args.max_rows,
        ),
    )
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
