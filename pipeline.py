from __future__ import annotations

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
from prefect import flow, task

COLUMN_RE = re.compile(r"[^a-z0-9_]+")


@dataclass(frozen=True)
class PipelinePolicy:
    required_columns: tuple[str, ...] = ()
    drop_null_columns: tuple[str, ...] = ()
    dedupe_keys: tuple[str, ...] = ()
    max_rows: int = 100_000

    def __post_init__(self) -> None:
        if not 1 <= self.max_rows <= 5_000_000:
            raise ValueError("max_rows must be between 1 and 5,000,000")


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
    input_digest: str
    output_digest: str
    output_path: str
    manifest_path: str


def normalize_column_name(value: object) -> str:
    text = str(value).strip().lower().replace(" ", "_")
    text = COLUMN_RE.sub("_", text).strip("_")
    if not text:
        raise ValueError("column name normalizes to an empty identifier")
    return text


def _records_for_digest(frame: pd.DataFrame) -> list[dict[str, Any]]:
    clean = frame.astype(object).where(pd.notna(frame), None)
    return clean.to_dict(orient="records")


def stable_digest(records: Iterable[dict[str, Any]]) -> str:
    encoded = json.dumps(
        list(records),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@task
def load_records(records: list[dict[str, Any]], max_rows: int = 100_000) -> pd.DataFrame:
    if len(records) > max_rows:
        raise ValueError(f"input exceeds max_rows={max_rows}")
    if any(not isinstance(record, dict) for record in records):
        raise TypeError("every input record must be an object")
    return pd.DataFrame.from_records(records)


@task
def normalize(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy(deep=True)
    normalized = [normalize_column_name(column) for column in result.columns]
    if len(normalized) != len(set(normalized)):
        raise ValueError("column normalization creates duplicate names")
    result.columns = normalized
    return result


def validate_required_columns(df: pd.DataFrame, required: tuple[str, ...]) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(f"required columns missing: {', '.join(missing)}")


def drop_null_rows(df: pd.DataFrame, columns: list[str] | tuple[str, ...] | None = None) -> pd.DataFrame:
    return df.dropna(subset=list(columns) if columns else None).reset_index(drop=True)


def deduplicate(df: pd.DataFrame, keys: tuple[str, ...]) -> pd.DataFrame:
    if not keys:
        return df.reset_index(drop=True)
    missing = sorted(set(keys) - set(df.columns))
    if missing:
        raise ValueError(f"dedupe columns missing: {', '.join(missing)}")
    return df.drop_duplicates(subset=list(keys), keep="first").reset_index(drop=True)


def run_transforms(
    df: pd.DataFrame,
    transforms: list[Callable[[pd.DataFrame], pd.DataFrame]] | None = None,
) -> pd.DataFrame:
    current = df.copy(deep=True)
    for transform in transforms or []:
        current = transform(current.copy(deep=True))
        if not isinstance(current, pd.DataFrame):
            raise TypeError("pipeline transforms must return pandas.DataFrame")
    return current


def process_records(
    records: list[dict[str, Any]],
    policy: PipelinePolicy | None = None,
    transforms: list[Callable[[pd.DataFrame], pd.DataFrame]] | None = None,
) -> PipelineResult:
    active = policy or PipelinePolicy()
    frame = normalize.fn(load_records.fn(records, active.max_rows))
    validate_required_columns(frame, active.required_columns)
    rows_in = len(frame)
    input_digest = stable_digest(_records_for_digest(frame))
    frame = drop_null_rows(frame, active.drop_null_columns)
    frame = deduplicate(frame, active.dedupe_keys)
    frame = run_transforms(frame, transforms)
    output_records = _records_for_digest(frame)
    return PipelineResult(
        frame=frame,
        rows_in=rows_in,
        rows_out=len(frame),
        rows_dropped=rows_in - len(frame),
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
                raise ValueError(f"line {line_number} must contain a JSON object")
            records.append(value)
    return records


def _atomic_json_write(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, sort_keys=True, indent=2, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def _atomic_jsonl_write(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True, default=str))
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def run_jsonl_job(
    source_path: str | Path,
    output_path: str | Path,
    manifest_path: str | Path,
    policy: PipelinePolicy | None = None,
) -> JobResult:
    active = policy or PipelinePolicy()
    source = Path(source_path).resolve()
    output = Path(output_path).resolve()
    manifest = Path(manifest_path).resolve()
    records = read_jsonl(source, active.max_rows)
    result = process_records(records, active)

    if manifest.exists() and output.exists():
        previous = json.loads(manifest.read_text(encoding="utf-8"))
        if (
            previous.get("input_digest") == result.input_digest
            and previous.get("policy") == asdict(active)
        ):
            return JobResult(
                status="replayed",
                rows_in=int(previous["rows_in"]),
                rows_out=int(previous["rows_out"]),
                input_digest=previous["input_digest"],
                output_digest=previous["output_digest"],
                output_path=str(output),
                manifest_path=str(manifest),
            )

    output_records = _records_for_digest(result.frame)
    _atomic_jsonl_write(output, output_records)
    manifest_data = {
        "version": 1,
        "source": str(source),
        "output": str(output),
        "policy": asdict(active),
        "rows_in": result.rows_in,
        "rows_out": result.rows_out,
        "rows_dropped": result.rows_dropped,
        "input_digest": result.input_digest,
        "output_digest": result.output_digest,
    }
    _atomic_json_write(manifest, manifest_data)
    return JobResult(
        status="completed",
        rows_in=result.rows_in,
        rows_out=result.rows_out,
        input_digest=result.input_digest,
        output_digest=result.output_digest,
        output_path=str(output),
        manifest_path=str(manifest),
    )


@flow(name="sky-dataflow")
def run_pipeline(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return _records_for_digest(process_records(records).frame)


if __name__ == "__main__":
    print(run_pipeline([{"Example Field": 1}]))
