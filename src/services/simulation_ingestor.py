from pathlib import Path
import polars as pl
from src.utils import configLogger
from src.schemas import SIMULATION_SCHEMA

class SimulationIngestionError(Exception):
    """Custom exception for ingestion failures."""
    pass

class SimulationIngestor:
    """
    Handles the ingestion, validation, cleaning, and artifact generation 
    of raw aero engine telemetry data.
    """
    
    def __init__(self, input_path: Path, artifact_path: Path):
        self.input_path = input_path
        self.artifact_path = artifact_path
        self.logger = configLogger(self.__class__.__name__)
        self.logger.info(f"Initialized ingestor. Input: {self.input_path.name} | Artifact: {self.artifact_path.name}")

    def _clean_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Applies physics-safe data cleaning rules."""
        self.logger.debug(f"Applying data cleaning to {df.height} rows...")
        # Inside your ingestor's _clean_data method
        initial_rows = df.height
        null_counts = df.null_count()
        total_nulls = sum(null_counts.row(0))

        if total_nulls > 0:
            self.logger.warning(f"Schema Audit: Found {total_nulls} corrupted/null fields across {initial_rows} rows. Initiating forward-fill coercion.")

        # Forward-fill minor sensor drops
        df = df.fill_null(strategy="forward")
        
        # Zero-fill remaining nulls (e.g., missing data at t=0)
        df = df.fill_null(0.0)
        
        df = df.rename({"t":"time"})
        return df

    def ingest_and_save(self) -> None:
        """
        Executes the ingestion pipeline and saves the clean data as a Parquet artifact.
        """
        self.logger.info(f"Starting ingestion process for: {self.input_path}")
        
        try:
            self.logger.debug("Reading CSV and enforcing SIMULATION_SCHEMA...")
            df = pl.read_csv(
                self.input_path,
                schema_overrides=SIMULATION_SCHEMA,
                null_values=["NA", "NaN", "null", ""],
                encoding='UTF-16'
            )
            
            if df.height == 0:
                self.logger.error("Ingestion failed: Dataframe is empty.")
                raise SimulationIngestionError("The uploaded simulation file is empty.")
                
            # Clean the dataframe
            clean_df = self._clean_data(df)

            # EXPLICIT SORT: Guarantees row-group statistics don't overlap.
            # This enables Polars to skip reading 90% of the file during zoomed-in queries.
            clean_df = clean_df.sort("time")

            self.logger.debug(f"Writing sorted artifact to {self.artifact_path}...")
            # Create parent directories if they don't exist
            self.artifact_path.parent.mkdir(parents=True, exist_ok=True)
            # Ensure statistics are generated for predicate pushdown
            clean_df.write_parquet(self.artifact_path, statistics=True)
            
            self.logger.info(f"Successfully generated clean telemetry artifact: {self.artifact_path}")
            

        except pl.exceptions.ColumnNotFoundError as e:
            self.logger.error(f"Missing required sensor columns: {str(e)}")
            raise SimulationIngestionError(f"Missing required sensor columns: {str(e)}")
            
        except pl.exceptions.SchemaError as e:
            self.logger.error(f"Data type mismatch: {str(e)}")
            raise SimulationIngestionError(f"Data type mismatch in simulation file: {str(e)}")
            
        except Exception as e:
            self.logger.exception("Unexpected error occurred during simulation ingestion.")
            raise SimulationIngestionError(f"Failed to parse telemetry data: {str(e)}")