from src.services import ingest_simulation_csv
from pathlib import Path

path = Path(r"data\run_1_cc554525120.csv")
ingest_simulation_csv(path)