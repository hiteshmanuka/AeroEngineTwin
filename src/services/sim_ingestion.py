from src.utils.logger import configLogger
import polars as pl
from pathlib import Path
from io import BytesIO
from schemas.simulation import SIMULATION_SCHEMA

logger = configLogger(__file__)

class SimulationIngestionError(Exception):
    """Custom exception for ingestion failures."""
    pass

def ingest_simulation_csv(file_path: Path) -> pl.DataFrame:
    """
    Ingests a CSV file into a highly optimized Polars DataFrame.
    Enforces Float32 schema and handles missing values.
    """
    logger.info(f"Starting simulation ingestion for file: {file_path}")
    
    try:
        logger.debug("Reading CSV with Polars and applying SIMULATION_SCHEMA...")
        
        # read_csv with strict schema enforces column names and types instantly
        df = pl.read_csv(
            file_path,
            schema_overrides=SIMULATION_SCHEMA,
            strict=True, #type:ignore
            null_values=["NA", "NaN", "null", ""],
            encoding='UTF-16'
        )
        
        # Check if the DataFrame is empty
        if df.height == 0:
            logger.error(f"Ingestion failed: The simulation file '{file_path}' is empty.")
            raise SimulationIngestionError("The uploaded simulation file is empty.")
            
        logger.debug(f"CSV read successfully. Rows: {df.height}. Applying data cleaning...")
        
        # Basic Data Cleaning: Forward-fill missing sensor data up to a limit
        # (e.g., if a sensor drops for a fraction of a second, carry the last value forward)
        df = df.fill_null(strategy="forward")
        
        # If there are still nulls (e.g., at the very start of the file), fill with 0.0
        df = df.fill_null(0.0)
        
        logger.info(f"Successfully ingested and cleaned simulation data. Final shape: {df.shape}")
        return df

    except pl.exceptions.ColumnNotFoundError as e:
        logger.error(f"ColumnNotFoundError during ingestion: {str(e)}")
        raise SimulationIngestionError(f"Missing required sensor columns: {str(e)}")
        
    except pl.exceptions.SchemaError as e:
        logger.error(f"SchemaError during ingestion: {str(e)}")
        raise SimulationIngestionError(f"Data type mismatch in simulation file: {str(e)}")
        
    except Exception as e:
        # logger.exception automatically includes the full traceback for debugging
        logger.exception("Unexpected error occurred during simulation ingestion.")
        raise SimulationIngestionError(f"Failed to parse telemetry data: {str(e)}")