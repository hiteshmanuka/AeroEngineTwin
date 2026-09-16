"""Builds a combined cylinder-balance feature table from all archived runs."""

import concurrent.futures
import polars as pl

from config import DATABASE_URL
from src.schemas import SENSOR_COLUMNS
from src.analytics.trend_analysis.cylinder_balance import build_cylinder_balance_features
from src.utils import configLogger

logger = configLogger(__file__)

OUTPUT_PATH = "data/features/cylinder_balance_feature_table.parquet"
MAX_WORKERS = 4


def _fetch_run_ids() -> list[str]:
    """Retrieves every archived run_id from simulation_runs."""
    df = pl.read_database_uri(
        query="SELECT run_id FROM simulation_runs;",
        uri=DATABASE_URL,
    )
    return df["run_id"].cast(pl.Utf8).to_list()


def _process_run(run_id: str) -> pl.DataFrame:
    """Fetches one run's telemetry and applies the cylinder-balance pipeline."""
    columns = ", ".join(SENSOR_COLUMNS)
    query = (
        f"SELECT t, {columns} FROM simulations "
        f"WHERE run_id = '{run_id}' ORDER BY t;"
    )
    df = pl.read_database_uri(query=query, uri=DATABASE_URL)
    features = build_cylinder_balance_features(df)
    features = features.with_columns(pl.lit(run_id).alias("run_id"))
    logger.info(f"Processed run_id={run_id}: {features.height} feature rows.")
    return features


def build_dataset() -> None:
    """Builds the combined cylinder-balance feature table and writes it to
    OUTPUT_PATH."""
    run_ids = _fetch_run_ids()

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        all_features = list(pool.map(_process_run, run_ids))

    combined = pl.concat(all_features)
    combined.write_parquet(OUTPUT_PATH)
    logger.info(f"Wrote {combined.height} total rows to {OUTPUT_PATH}.")


if __name__ == "__main__":
    build_dataset()