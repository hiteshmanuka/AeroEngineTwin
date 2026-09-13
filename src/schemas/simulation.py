import polars as pl
from pydantic import BaseModel

class SimulationRow(BaseModel):
    time: float
    throttle: float
    alt_m: float
    ambient_c: float
    airspeed_ms: float
    cool_index: float
    rpm: float
    map_kpa: float
    torque_nm: float
    cht_c: float
    egt_c: float
    oil_c: float
    cht_c_1: float
    cht_c_2: float
    cht_c_3: float
    cht_c_4: float
    egt_c_1: float
    egt_c_2: float
    egt_c_3: float
    egt_c_4: float
    air_gps: float
    fuel_kgph: float
    fuel_press_kpa: float
    lambda_1: float
    lambda_2: float
    lambda_3: float
    lambda_4: float
    oil_press_kpa: float
    bus_v: float
    alt_a: float
    alt_field_a: float
    batt_soc: float

# This is the strict schema Polars will use during file I/O
SIMULATION_SCHEMA = {
    field: pl.Float32 for field in SimulationRow.model_fields.keys()
}