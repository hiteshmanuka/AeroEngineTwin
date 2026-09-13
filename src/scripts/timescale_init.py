import os
import psycopg2
from src.utils import configLogger

logger = configLogger(__file__)

PG_CONFIG = {
    "dbname": os.environ.get("POSTGRES_DB"),
    "user": os.environ.get("POSTGRES_USER"),
    "password": os.environ.get("POSTGRES_PASSWORD"),
    "host": os.environ.get("POSTGRES_HOST"),
    "port": os.environ.get("POSTGRES_PORT"),
}


def init_timescaledb():
    """Idempotent setup for the TimescaleDB schema and hypertable."""
    logger.info("--- Connecting to TimescaleDB ---")
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS simulations (
            run_id VARCHAR(50) NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),  -- monotonic dimension for chunking
            time DOUBLE PRECISION NOT NULL,                   -- sim-elapsed seconds, resets per run
            throttle REAL, alt_m REAL, ambient_c REAL, airspeed_ms REAL,
            cool_index REAL, rpm REAL, map_kpa REAL, torque_nm REAL,
            cht_c REAL, egt_c REAL, oil_c REAL,
            cht_c_1 REAL, cht_c_2 REAL, cht_c_3 REAL, cht_c_4 REAL,
            egt_c_1 REAL, egt_c_2 REAL, egt_c_3 REAL, egt_c_4 REAL,
            air_gps REAL, fuel_kgph REAL, fuel_press_kpa REAL,
            lambda_1 REAL, lambda_2 REAL, lambda_3 REAL, lambda_4 REAL,
            oil_press_kpa REAL, bus_v REAL, alt_a REAL, alt_field_a REAL, batt_soc REAL,
            UNIQUE (run_id, time)   -- prevents silent duplicate ingestion of the same run
        );
        """)

        # Partition by ingested_at (wall clock, always increasing), NOT sim time
        cursor.execute("""
        SELECT create_hypertable(
            'simulations',
            'ingested_at',
            chunk_time_interval => INTERVAL '1 day',
            if_not_exists => TRUE
        );
        """)

        # This is the index your /simulations endpoint's WHERE run_id=X AND time BETWEEN a AND b
        # actually uses — chunk exclusion on ingested_at won't help a single-run query,
        # this index is what makes that query fast.
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS ix_simulations_run_time
        ON simulations (run_id, time);
        """)

        logger.info("TimescaleDB schema & hypertable initialized.")

    except Exception as e:
        logger.error(f"TimescaleDB initialization failed: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    print("Initializing infrastructure...")
    init_timescaledb()
    print("TimescaleDB infrastructure primed.")