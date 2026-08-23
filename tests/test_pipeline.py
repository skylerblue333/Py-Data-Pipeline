import pandas as pd
import pytest

from pipeline import drop_null_rows, normalize, run_transforms


def test_normalize_columns():
    result = normalize.fn(pd.DataFrame({" Customer Name ": ["Sky"]}))
    assert list(result.columns) == ["customer_name"]


def test_transform_pipeline_tracks_rows_without_mutating_input():
    source = pd.DataFrame({"value": [1, None, 3]})
    result = run_transforms(source, [lambda df: drop_null_rows(df, ["value"])])
    assert result.rows_in == 3
    assert result.rows_out == 2
    assert len(source) == 3


def test_transform_must_return_dataframe():
    with pytest.raises(TypeError):
        run_transforms(pd.DataFrame({"value": [1]}), [lambda _: [1]])
