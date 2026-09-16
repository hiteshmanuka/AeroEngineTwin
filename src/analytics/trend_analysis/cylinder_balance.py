"""Cylinder balance feature engineering."""

import polars as pl

CYLINDER_GROUPS = {
    "cht_c": ["cht_c_1", "cht_c_2", "cht_c_3", "cht_c_4"],
    "egt_c": ["egt_c_1", "egt_c_2", "egt_c_3", "egt_c_4"],
    "lambda": ["lambda_1", "lambda_2", "lambda_3", "lambda_4"],
}


def add_cylinder_spread(df: pl.DataFrame) -> pl.DataFrame:
    """Adds a max-min spread column per cylinder group.

    Args:
        df: Telemetry DataFrame containing the columns in CYLINDER_GROUPS.

    Returns:
        DataFrame with `{group}_spread` columns added.
    """
    exprs = [
        (pl.max_horizontal(cols) - pl.min_horizontal(cols)).alias(f"{name}_spread")
        for name, cols in CYLINDER_GROUPS.items()
    ]
    return df.with_columns(exprs)


def add_cylinder_deviation(df: pl.DataFrame) -> pl.DataFrame:
    """Adds per-cylinder deviation from its bank mean.

    Args:
        df: Telemetry DataFrame containing the columns in CYLINDER_GROUPS.

    Returns:
        DataFrame with `{cylinder_column}_dev` columns added.
    """
    exprs = []
    for name, cols in CYLINDER_GROUPS.items():
        bank_mean = pl.mean_horizontal(cols)
        exprs.extend((pl.col(c) - bank_mean).alias(f"{c}_dev") for c in cols)
    return df.with_columns(exprs)


def add_same_cylinder_correlation(df: pl.DataFrame, window_size: int = 20) -> pl.DataFrame:
    """Adds a rolling CHT-EGT correlation per cylinder index.

    Args:
        df: Telemetry DataFrame containing cht_c_1..4 and egt_c_1..4.
        window_size: Rolling window length in rows.

    Returns:
        DataFrame with `cyl{i}_cht_egt_corr` columns added for i in 1..4.
    """
    exprs = []
    for i in range(1, 5):
        x, y = pl.col(f"cht_c_{i}"), pl.col(f"egt_c_{i}")
        mean_x = x.rolling_mean(window_size)
        mean_y = y.rolling_mean(window_size)
        mean_xy = (x * y).rolling_mean(window_size)
        cov = mean_xy - mean_x * mean_y
        corr = cov / (x.rolling_std(window_size) * y.rolling_std(window_size))
        exprs.append(corr.alias(f"cyl{i}_cht_egt_corr"))
    return df.with_columns(exprs)


def build_cylinder_balance_features(df: pl.DataFrame, window_size: int = 20) -> pl.DataFrame:
    """Runs the full cylinder-balance feature pipeline on one run's telemetry.

    Args:
        df: Raw telemetry DataFrame for one run, sorted by `t`.
        window_size: Rolling window length in rows, used for correlation.

    Returns:
        Feature DataFrame with spread, deviation, and correlation columns
        added, with incomplete-window rows removed.
    """
    df = add_cylinder_spread(df)
    df = add_cylinder_deviation(df)
    df = add_same_cylinder_correlation(df, window_size)
    return df.drop_nulls()


def find_spread_divergence_lag(
    df: pl.DataFrame, max_lag: int = 10
) -> dict[str, dict[str, float]]:
    """Determines whether lambda_spread leads or lags cht_c_spread/egt_c_spread.

    Args:
        df: Feature DataFrame that already has lambda_spread, cht_c_spread,
            and egt_c_spread columns (from add_cylinder_spread).
        max_lag: Maximum shift, in rows, tested in either direction.

    Returns:
        A dict keyed by target spread name, each mapping to the lag (rows;
        negative means lambda_spread leads) with the strongest correlation
        and that correlation value.
    """
    results: dict[str, dict[str, float]] = {}
    for target in ("cht_c_spread", "egt_c_spread"):
        best_lag, best_corr = 0, 0.0
        for lag in range(-max_lag, max_lag + 1):
            shifted = pl.DataFrame({
                "lambda_shifted": df["lambda_spread"].shift(lag),
                "target": df[target],
            }).drop_nulls()
            if shifted.height < 2:
                continue
            corr = shifted.select(pl.corr("lambda_shifted", "target")).item()
            if corr is not None and abs(corr) > abs(best_corr):
                best_lag, best_corr = lag, corr
        results[target] = {"lag": best_lag, "correlation": best_corr}
    return results