import time
import polars as pl
from pathlib import Path
from src.utils import configLogger

class SimulationDownsampler:
    """
    On-demand telemetry query service.
    Reads strictly from the validated full-resolution Parquet artifact using LazyFrames.
    Applies M4 downsampling only when the requested time slice exceeds the UI resolution limit.
    """
    
    def __init__(self, artifact_path: Path):
        self.artifact_path = artifact_path
        self.logger = configLogger(self.__class__.__name__)
        self.logger.info(f"Initialized On-Demand Downsampler mapped to artifact: {self.artifact_path}")

    def query_time_window(self, start_time: float, end_time: float, target_buckets: int) -> pl.DataFrame:
        """
        Queries a specific time window and dynamically applies M4 downsampling if required.
        """
        self.logger.info(f"Query received: t=[{start_time}s to {end_time}s], Target Resolution: {target_buckets}px")
        query_start_ms = time.perf_counter()

        try:
            # 1. LAZY EXECUTION: Scan the parquet and filter before pulling into RAM
            lazy_query = (
                pl.scan_parquet(self.artifact_path)
                .filter((pl.col("time") >= start_time) & (pl.col("time") <= end_time))
            )
            
            # Collect only the requested time slice into memory
            df_slice = lazy_query.collect()
            slice_rows = df_slice.height
            
            self.logger.debug(f"Retrieved {slice_rows} raw rows for requested time window.")

            if slice_rows == 0:
                self.logger.warning("Query returned 0 rows. Check time bounds.")
                return df_slice

            # 2. THE BYPASS SWITCH: Fixes the 1:1 compression ratio bug
            # M4 returns 4 points per bucket. If we have fewer rows than the max possible 
            # downsampled output, M4 is useless. Return raw data immediately.
            max_m4_points = target_buckets * 4
            
            if slice_rows <= max_m4_points:
                self.logger.info(
                    f"Bypass triggered: Data density ({slice_rows} rows) is lower than UI limit "
                    f"({max_m4_points} points). Returning raw full-resolution data."
                )
                return df_slice

            # 3. M4 AGGREGATION: With Performance Instrumentation
            self.logger.debug(f"Data density exceeds UI limits. Commencing M4 Aggregation...")
            agg_start_ms = time.perf_counter()
            
            # Determine precise bucket width for the filtered slice
            slice_min_time = df_slice.select(pl.col("time").min()).item()
            slice_max_time = df_slice.select(pl.col("time").max()).item()
            bucket_width = (slice_max_time - slice_min_time) / target_buckets
            
            # Assign bucket IDs
            df_slice = df_slice.with_columns(
                ((pl.col("time") - slice_min_time) / bucket_width).cast(pl.Int32).alias("bucket")
            )
            
            sensors = [col for col in df_slice.columns if col not in ["time", "bucket"]]
            
            # Build expressions
            aggs = [pl.col("time").first().alias("bucket_start_time")]
            for s in sensors:
                aggs.extend([
                    pl.col(s).first().alias(f"{s}_first"),
                    pl.col(s).min().alias(f"{s}_min"),
                    pl.col(s).max().alias(f"{s}_max"),
                    pl.col(s).last().alias(f"{s}_last"),
                ])
                
            # Execute aggregations in parallel
            bucketed = df_slice.group_by("bucket").agg(aggs)
            
            # Unpack into 4 chronological steps
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
            
            # Rebuild and clean up
            final_df = pl.concat([df_first, df_min, df_max, df_last]).sort(["bucket", "step"]).drop(["bucket", "step"])
            
            # Stop timer and log performance
            agg_duration = (time.perf_counter() - agg_start_ms) * 1000
            total_duration = (time.perf_counter() - query_start_ms) * 1000
            
            self.logger.info(
                f"M4 Aggregation complete. Compressed {slice_rows} -> {final_df.height} rows. "
                f"Agg time: {agg_duration:.2f}ms | Total query time: {total_duration:.2f}ms"
            )
            
            return final_df

        except Exception as e:
            self.logger.error(f"Fatal error during telemetry query: {str(e)}", exc_info=True)
            raise