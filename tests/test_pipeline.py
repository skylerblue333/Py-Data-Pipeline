import json

import pandas as pd
import pytest

from pipeline import (
    PipelinePolicy,
    drop_null_rows,
    normalize,
    process_records,
    run_jsonl_job,
    run_transforms,
)


def test_normalize_columns():
    result = normalize.fn(pd.DataFrame({" Customer Name ": ["Sky"]}))
    assert list(result.columns) == ["customer_name"]


def test_normalize_rejects_collisions():
    with pytest.raises(ValueError, match="duplicate names"):
        normalize.fn(pd.DataFrame({"A B": [1], "a_b": [2]}))


def test_policy_drops_nulls_and_deduplicates():
    records = [
        {"ID": 1, "Value": "a"},
        {"ID": 1, "Value": "duplicate"},
        {"ID": 2, "Value": None},
    ]
    policy = PipelinePolicy(
        required_columns=("id", "value"),
        drop_null_columns=("value",),
        dedupe_keys=("id",),
    )
    result = process_records(records, policy)
    assert result.rows_in == 3
    assert result.rows_out == 1
    assert result.rows_dropped == 2
    assert result.frame.iloc[0].to_dict() == {"id": 1, "value": "a"}
    assert len(result.input_digest) == 64
    assert len(result.output_digest) == 64


def test_transform_pipeline_does_not_mutate_input():
    source = pd.DataFrame({"value": [1, None, 3]})
    transformed = run_transforms(source, [lambda df: drop_null_rows(df, ["value"])])
    assert len(transformed) == 2
    assert len(source) == 3


def test_transform_must_return_dataframe():
    with pytest.raises(TypeError):
        run_transforms(pd.DataFrame({"value": [1]}), [lambda _: [1]])


def test_jsonl_job_is_atomic_and_replayable(tmp_path):
    source = tmp_path / "input.jsonl"
    output = tmp_path / "output.jsonl"
    manifest = tmp_path / "manifest.json"
    source.write_text('{"ID":1,"Value":"a"}\n{"ID":1,"Value":"b"}\n', encoding="utf-8")
    policy = PipelinePolicy(required_columns=("id", "value"), dedupe_keys=("id",))

    first = run_jsonl_job(source, output, manifest, policy)
    second = run_jsonl_job(source, output, manifest, policy)

    assert first.status == "completed"
    assert second.status == "replayed"
    assert output.read_text(encoding="utf-8").count("\n") == 1
    metadata = json.loads(manifest.read_text(encoding="utf-8"))
    assert metadata["rows_in"] == 2
    assert metadata["rows_out"] == 1
    assert metadata["output_digest"] == first.output_digest


def test_jsonl_job_rejects_malformed_input(tmp_path):
    source = tmp_path / "input.jsonl"
    source.write_text('{"ok":1}\nnot-json\n', encoding="utf-8")
    with pytest.raises(ValueError, match="line 2"):
        run_jsonl_job(source, tmp_path / "out.jsonl", tmp_path / "manifest.json")


def test_row_limit_is_enforced():
    policy = PipelinePolicy(max_rows=1)
    with pytest.raises(ValueError, match="max_rows"):
        process_records([{"id": 1}, {"id": 2}], policy)
