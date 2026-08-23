from prefect import flow, task
import pandas as pd

@task

def load_records(records: list[dict]) -> pd.DataFrame:
    return pd.DataFrame.from_records(records)

@task

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result.columns = [str(c).strip().lower().replace(" ", "_") for c in result.columns]
    return result

@flow(name="skycoin-data-pipeline")
def run_pipeline(records: list[dict]) -> list[dict]:
    return normalize(load_records(records)).to_dict(orient="records")

if __name__ == "__main__":
    print(run_pipeline([{"Example Field": 1}]))
