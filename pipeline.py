from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd
from prefect import flow, task


@dataclass(frozen=True)
class PipelineResult:
    frame: pd.DataFrame
    rows_in: int
    rows_out: int


@task

def load_records(records: list[dict]) -> pd.DataFrame:
    return pd.DataFrame.from_records(records)


@task

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy(deep=True)
    result.columns = [str(c).strip().lower().replace(" ", "_") for c in result.columns]
    return result


def run_transforms(df: pd.DataFrame, transforms: list[Callable[[pd.DataFrame], pd.DataFrame]] | None = None) -> PipelineResult:
    current = df.copy(deep=True)
    rows_in = len(current)
    for transform in transforms or []:
        current = transform(current.copy(deep=True))
        if not isinstance(current, pd.DataFrame):
            raise TypeError("pipeline transforms must return pandas.DataFrame")
    return PipelineResult(frame=current, rows_in=rows_in, rows_out=len(current))


def drop_null_rows(df: pd.DataFrame, columns: list[str] | None = None) -> pd.DataFrame:
    return df.dropna(subset=columns).reset_index(drop=True)


@flow(name="skycoin-data-pipeline")
def run_pipeline(records: list[dict]) -> list[dict]:
    return normalize(load_records(records)).to_dict(orient="records")


if __name__ == "__main__":
    print(run_pipeline([{"Example Field": 1}]))
