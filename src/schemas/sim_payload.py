from pydantic import BaseModel
from typing import Dict, List, Literal

class TimeRange(BaseModel):
    start: float
    end: float
    step: float

class ResolutionMeta(BaseModel):
    mode: Literal["raw", "m4"]
    target_buckets: int
    point_count: int

class SimulationMetadata(BaseModel):
    run_id: str
    time_range_served: TimeRange
    resolution: ResolutionMeta

class SimulationData(BaseModel):
    # time: List[float]
    channels: Dict[str, List[float]]

class SimulationResponse(BaseModel):
    metadata: SimulationMetadata
    data: SimulationData