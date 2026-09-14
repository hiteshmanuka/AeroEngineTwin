import time
import polars as pl
from pathlib import Path
from src.utils import configLogger

class SimulationDownsampler:
    def __init__(self):
        self.logger = configLogger(self.__class__.__name__)
        
    def query_dataframe(self, df_slice: pl.DataFrame, target_buckets: int) -> tuple[pl.DataFrame, str]:
        """
        Shared bypass-check + M4 aggregation. Takes an already-fetched
        DataFrame — source-agnostic, so 
        a Postgres-fetched DataFrame (telemetry endpoint) hit identical
        aggregation logic. Returns (result_df, mode) so callers can pass
        mode straight into serialize_telemetry without re-deriving it.
        """
        query_start_ms = time.perf_counter()
        slice_rows = df_slice.height

        if slice_rows == 0:
            self.logger.warning("Query returned 0 rows. Check time bounds.")
            return df_slice.rename({"t": "time"}), "raw"

        max_m4_points = target_buckets * 4
        if slice_rows <= max_m4_points:
            self.logger.info(f"Bypass triggered: {slice_rows} rows <= {max_m4_points} limit.")
            return df_slice.rename({"t": "time"}), "raw"

        self.logger.debug("Data density exceeds UI limits. Commencing M4 Aggregation...")
        agg_start_ms = time.perf_counter()

        slice_min_time = df_slice.select(pl.col("t").min()).item()
        slice_max_time = df_slice.select(pl.col("t").max()).item()
        bucket_width = (slice_max_time - slice_min_time) / target_buckets

        df_slice = df_slice.with_columns(
            ((pl.col("t") - slice_min_time) / bucket_width).cast(pl.Int32).alias("bucket")
        )
        sensors = [c for c in df_slice.columns if c not in ["t", "bucket"]]

        aggs = [pl.col("t").first().alias("bucket_start_time")]
        for s in sensors:
            aggs.extend([
                pl.col(s).first().alias(f"{s}_first"),
                pl.col(s).min().alias(f"{s}_min"),
                pl.col(s).max().alias(f"{s}_max"),
                pl.col(s).last().alias(f"{s}_last"),
            ])
        bucketed = df_slice.group_by("bucket").agg(aggs)

        df_first = bucketed.select(pl.col("bucket"), pl.lit(1).alias("step"),
            pl.col("bucket_start_time").alias("time"),
            *[pl.col(f"{s}_first").alias(s) for s in sensors])
        df_min = bucketed.select(pl.col("bucket"), pl.lit(2).alias("step"),
            (pl.col("bucket_start_time") + bucket_width * 0.33).alias("time"),
            *[pl.col(f"{s}_min").alias(s) for s in sensors])
        df_max = bucketed.select(pl.col("bucket"), pl.lit(3).alias("step"),
            (pl.col("bucket_start_time") + bucket_width * 0.66).alias("time"),
            *[pl.col(f"{s}_max").alias(s) for s in sensors])
        df_last = bucketed.select(pl.col("bucket"), pl.lit(4).alias("step"),
            (pl.col("bucket_start_time") + bucket_width * 0.99).alias("time"),
            *[pl.col(f"{s}_last").alias(s) for s in sensors])

        final_df = pl.concat([df_first, df_min, df_max, df_last]).sort(["bucket", "step"]).drop(["bucket", "step"])

        agg_duration = (time.perf_counter() - agg_start_ms) * 1000
        total_duration = (time.perf_counter() - query_start_ms) * 1000
        self.logger.info(
            f"M4 Agg: {agg_duration:.2f}ms | Total: {total_duration:.2f}ms | "
            f"Compression: {slice_rows} -> {final_df.height} rows."
        )
        return final_df, "m4"