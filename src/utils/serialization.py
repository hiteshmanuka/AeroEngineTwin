import polars as pl

def serialize_telemetry(df: pl.DataFrame, run_id: str, mode: str, target_buckets: int) -> dict:
    """
    Rounds float precision and converts a Polars DataFrame into a 
    columnar Structure-of-Arrays dictionary for Apache ECharts.
    """
    point_count = df.height
    
    if point_count == 0:
        return {} # Handle empty state gracefully
        
    # 1. Server-Side Float Rounding (Vectorized in Rust)
    # Time gets 3 decimals (milliseconds), sensors get 2 decimals
    df = df.with_columns([
        pl.col("time").round(3),
        pl.exclude("time").round(2)
    ])
    
    # 2. Extract actual time range served
    time_min = df.select(pl.col("time").first()).item()
    time_max = df.select(pl.col("time").last()).item()
    
    # 3. Columnar Conversion
    # as_series=False instantly converts the Polars columns to flat Python lists
    data_dict = df.to_dict(as_series=False)
    
    # Pop time out so the rest of the dict is strictly channels
    time_array = data_dict.pop("time")
    
    # 4. Build the exact envelope requested
    return {
        "metadata": {
            "run_id": run_id,
            "time_range_served": {
                "start": time_min,
                "end": time_max
            },
            "resolution": {
                "mode": mode,
                "target_buckets": target_buckets,
                "point_count": point_count
            }
        },
        "data": {
            "time": time_array,
            "channels": data_dict
        }
    }