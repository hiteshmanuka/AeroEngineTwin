import polars as pl
from src.utils import configLogger
from pathlib import Path

class SimulationDownsampler:
    """
    Handles data reduction for high-frequency aero engine simulation.
    Optimized for DRDO digital twin visual tracking without distorting physics.
    """
    
    def __init__(self, default_buckets: int, input_path: Path, artifact_path: Path):
        self.default_buckets = default_buckets
        self.input_path = input_path
        self.artifact_path = artifact_path
        self.df = pl.read_csv(self.input_path)
        self.logger = configLogger(self.__class__.__name__)
        self.logger.info(f"Simulation Downsampler initialized with {self.default_buckets} default buckets.")

    def save_df(self, df: pl.DataFrame) -> None:
        self.artifact_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(self.artifact_path)
        self.logger.info(f"Downsampled df saved at : {self.artifact_path}")
    
