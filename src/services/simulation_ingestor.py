from pathlib import Path
from typing import BinaryIO, Union
import polars as pl
import asyncpg
import io

from src.utils import configLogger
from src.schemas import SIMULATION_SCHEMA


class SimulationIngestionError(Exception):
    """Custom exception for ingestion failures."""
    pass


SENSOR_COLUMNS = [f for f in SIMULATION_SCHEMA.keys() if f != "t"]


class SimulationIngestor:
    """
    Handles the ingestion, validation, cleaning, and artifact generation
    of raw aero engine telemetry data.
    Supports TimescaleDB write (FastAPI ingestion endpoint, new).
    """

    def __init__(self, encoding : str = 'UTF-16'):
        self.logger = configLogger(self.__class__.__name__)
        self.encoding = encoding

    def _read_and_validate(self, source: Union[Path, BinaryIO]) -> pl.DataFrame:
        self.logger.debug("Reading CSV and enforcing SIMULATION_SCHEMA...")

        if isinstance(source, Path):
            raw_bytes = source.read_bytes()
        else:
            source.seek(0)  # in case anything upstream already read the stream
            raw_bytes = source.read()

        try:
            decoded_text = raw_bytes.decode(self.encoding)
        except UnicodeDecodeError as e:
            self.logger.error(f"{self.encoding} decode failed: {e}")
            raise SimulationIngestionError(f"File is not valid {self.encoding}: {e}")

        df = pl.read_csv(
            io.StringIO(decoded_text),
            schema_overrides=SIMULATION_SCHEMA,
            null_values=["NA", "NaN", "null", ""],
            # no `encoding=` param — the buffer is already str/unicode, nothing left to decode
        )
        if df.height == 0:
            self.logger.error("Ingestion failed: Dataframe is empty.")
            raise SimulationIngestionError("The uploaded simulation file is empty.")
        return df

    def _clean_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Applies physics-safe data cleaning rules. Unchanged from original."""
        self.logger.debug(f"Applying data cleaning to {df.height} rows...")
        initial_rows = df.height
        null_counts = df.null_count()
        total_nulls = sum(null_counts.row(0))

        if total_nulls > 0:
            self.logger.warning(
                f"Schema Audit: Found {total_nulls} corrupted/null fields "
                f"across {initial_rows} rows. Initiating forward-fill coercion."
            )

        df = df.fill_null(strategy="forward")
        df = df.fill_null(0.0)
        return df

    def _validate_and_clean(self, source: Union[Path, BinaryIO]) -> pl.DataFrame:
        """Full read -> validate -> clean -> sort pipeline, sink-agnostic."""
        try:
            df = self._read_and_validate(source)
            clean_df = self._clean_data(df)
            return clean_df.sort("t")
        except pl.exceptions.ColumnNotFoundError as e:
            self.logger.error(f"Missing required sensor columns: {str(e)}")
            raise SimulationIngestionError(f"Missing required sensor columns: {str(e)}")
        except pl.exceptions.SchemaError as e:
            self.logger.error(f"Data type mismatch: {str(e)}")
            raise SimulationIngestionError(f"Data type mismatch in simulation file: {str(e)}")
        except SimulationIngestionError:
            raise
        except Exception as e:
            self.logger.exception("Unexpected error occurred during simulation ingestion.")
            raise SimulationIngestionError(f"Failed to parse telemetry data: {str(e)}")

    async def ingest_to_db(
        self, run_id: str, source: Union[Path, BinaryIO], pool: asyncpg.Pool
    ) -> int:
        """
        FastAPI path — validates/cleans, then bulk-inserts into the
        `simulations` hypertable via asyncpg. Uses copy_records_to_table,
        not executemany — meaningfully faster at scale and the fix for
        the executemany bottleneck flagged earlier for longer runs.
        """
        self.logger.info(f"Starting DB ingestion for run_id={run_id}")
        clean_df = self._validate_and_clean(source)

        records = [
            (run_id, row["t"], *[row.get(c) for c in SENSOR_COLUMNS])
            for row in clean_df.iter_rows(named=True)
        ]
        columns = ["run_id", "t"] + SENSOR_COLUMNS

        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    # App-layer dedup, replacing the DB-level UNIQUE constraint
                    # Timescale rejected on this hypertable (partition column
                    # must be part of any unique index) — re-ingesting a run
                    # overwrites rather than duplicating.
                    await conn.execute("DELETE FROM simulations WHERE run_id = $1;", run_id)
                    await conn.copy_records_to_table(
                        "simulations", records=records, columns=columns
                    )
        except asyncpg.PostgresError as e:
            self.logger.exception("Database write failed during ingestion.")
            raise SimulationIngestionError(f"Failed to write telemetry to database: {str(e)}")

        self.logger.info(f"Successfully ingested {len(records)} rows for run_id={run_id}")
        return len(records)