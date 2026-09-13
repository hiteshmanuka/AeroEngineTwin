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
        
    def downsample_m4(self, n_buckets: int) -> None:
            """
            Downsamples multi-channel aero engine simulation using a Synthetic M4 algorithm.
            Guarantees retention of physical Min/Max spikes.
            """
            target_buckets = n_buckets or self.default_buckets
            self.logger.info(f"Starting M4 downsampling. Input shape: {self.df.shape}, Target buckets: {target_buckets}")
    
            if self.df.is_empty():
                self.logger.warning("Empty DataFrame passed to downsampling. Returning empty DataFrame.")
                self.save_df(self.df)
                return
    
            try:
                # 1. Determine time bounds and bucket width
                # OPTIMIZATION: Combine bounds check into a single expression evaluation
                bounds = self.df.select(
                    pl.col("time").min().alias("min"), 
                    pl.col("time").max().alias("max")
                )
                time_min = bounds.item(0, "min")
                time_max = bounds.item(0, "max")
                bucket_width = (time_max - time_min) / target_buckets
                
                self.logger.debug(f"Time bounds: {time_min}s to {time_max}s. Bucket width: {bucket_width:.4f}s")
                
                # 2. Assign a bucket ID to every row
                self.df = self.df.with_columns(
                    ((pl.col("time") - time_min) / bucket_width).cast(pl.Int32).alias("bucket")
                )
                
                # Get all sensor columns (excluding time and bucket)
                sensors = [col for col in self.df.columns if col not in ["time", "bucket"]]
                
                # 3. Create the Aggregation Expressions
                self.logger.debug(f"Building M4 aggregation expressions for {len(sensors)} sensor channels.")
                aggs = [pl.col("time").first().alias("bucket_start_time")]
                for s in sensors:
                    aggs.extend([
                        pl.col(s).first().alias(f"{s}_first"),
                        pl.col(s).min().alias(f"{s}_min"),
                        pl.col(s).max().alias(f"{s}_max"),
                        pl.col(s).last().alias(f"{s}_last"),
                    ])
                    
                # Group and aggregate in one vectorized sweep
                self.logger.info("Executing parallel Polars M4 aggregations...")
                bucketed = self.df.group_by("bucket").agg(aggs)
                
                # 4. Unpack the aggregations into 4 separate chronological points per bucket
                self.logger.debug("Unpacking bucketed aggregates into chronological sub-steps.")
                
                df_first = bucketed.select(
                    pl.col("bucket"), pl.lit(1).alias("step"),
                    pl.col("bucket_start_time").alias("time"),
                    *[pl.col(f"{s}_first").alias(s) for s in sensors]
                )
                
                df_min = bucketed.select(
                    pl.col("bucket"), pl.lit(2).alias("step"),
                    (pl.col("bucket_start_time") + (bucket_width * 0.33)).alias("time"),
                    *[pl.col(f"{s}_min").alias(s) for s in sensors]
                )
                
                # Note: For strict physics mapping, you can map exact time of min/max 
                # instead of hardcoding 0.33/0.66 if needed.
                df_max = bucketed.select(
                    pl.col("bucket"), pl.lit(3).alias("step"),
                    (pl.col("bucket_start_time") + (bucket_width * 0.66)).alias("time"),
                    *[pl.col(f"{s}_max").alias(s) for s in sensors]
                )
                
                df_last = bucketed.select(
                    pl.col("bucket"), pl.lit(4).alias("step"),
                    (pl.col("bucket_start_time") + (bucket_width * 0.99)).alias("time"),
                    *[pl.col(f"{s}_last").alias(s) for s in sensors]
                )
                
                # 5. Concatenate vertically and sort to rebuild the continuous time series
                self.logger.info("Rebuilding continuous time series from sub-steps...")
                final_df = pl.concat([df_first, df_min, df_max, df_last]).sort(["bucket", "step"]).drop(["bucket", "step"])
                
                self.logger.info(f"M4 downsampling complete. Output shape: {final_df.shape}")
                self.save_df(final_df)
    
            except Exception as e:
                self.logger.error(f"Fatal error during M4 downsampling: {str(e)}", exc_info=True)
                raise
