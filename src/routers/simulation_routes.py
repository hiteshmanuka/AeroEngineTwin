from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Request, Query
import io
import polars as pl
import time

from src.services import SimulationIngestor, SimulationIngestionError
from src.services import SimulationDownsampler
from src.utils import serialize_telemetry
from src.schemas import SimulationResponse, SENSOR_COLUMNS

from config import (ENCODING, SIM_BUCKET, 
                    MIN_BUCKETS, MAX_BUCKETS, DEFAULT_BUCKETS, 
                    DEFAULT_START, DEFAULT_END)

router = APIRouter()

@router.post("/ingest")
async def ingest_run(request: Request, run_id: str = Form(...), file: UploadFile = File(...)):
    raw_bytes = await file.read()  # read once, reused for both sinks below

    ingestor = SimulationIngestor(encoding=ENCODING)

    try:
        row_count = await ingestor.ingest_to_db(
            run_id, io.BytesIO(raw_bytes), request.app.state.db_pool
        )
    except SimulationIngestionError as e:
        raise HTTPException(status_code=422, detail=str(e))

    try:
        request.app.state.s3_client.put_object(
            Bucket=SIM_BUCKET,
            Key=f"{run_id}.csv",
            Body=raw_bytes,
        )
    except Exception as e:
        # DB write already succeeded — don't fail the whole request over
        # archive failure, but don't silently hide it either.
        ingestor.logger.error(f"Raw archive to SeaweedFS failed for run_id={run_id}: {e}")
        return {
            "run_id": run_id,
            "rows_ingested": row_count,
            "archived": False,
            "archive_error": str(e),
        }

    return {"run_id": run_id, "rows_ingested": row_count, "archived": True}

@router.get("/runs/{run_id}/telemetry", response_model=SimulationResponse)
async def get_telemetry(
    request: Request,
    run_id: str,
    start: float = Query(DEFAULT_START),
    end: float = Query(DEFAULT_END),
    target_buckets: int = Query(DEFAULT_BUCKETS),
    channels: str | None = Query(None, description="Comma-separated channel names; omit for all"),
):
    if end <= start:
        raise HTTPException(400, "end must be greater than start")
    target_buckets = max(MIN_BUCKETS, min(target_buckets, MAX_BUCKETS))

    io_start = time.perf_counter()
    async with request.app.state.db_pool.acquire() as conn:
        requested_channels = [c.strip() for c in channels.split(",")] if channels else SENSOR_COLUMNS
        invalid = set(requested_channels) - set(SENSOR_COLUMNS)
        if invalid:
            raise HTTPException(400, f"Unknown channels: {invalid}")
        col_list = ", ".join(["t"] + requested_channels)
        rows = await conn.fetch(
            f"SELECT {col_list} FROM simulations WHERE run_id = $1 AND t BETWEEN $2 AND $3 ORDER BY t;",
            run_id, start, end,
        )
    io_duration = (time.perf_counter() - io_start) * 1000

    if not rows:
        raise HTTPException(404, f"No data for run_id={run_id} in range [{start}, {end}]")

    df = pl.DataFrame([dict(r) for r in rows])

    cols_to_drop = [col for col in ["run_id", "ingested_at"] if col in df.columns]
    if cols_to_drop:
        df = df.drop(cols_to_drop)
        
    downsampler = SimulationDownsampler()
    result_df, mode = downsampler.query_dataframe(df, target_buckets)

    downsampler.logger.info(f"DB fetch for [{run_id}] {start}-{end}s: {io_duration:.2f}ms")

    return serialize_telemetry(result_df, run_id, mode, target_buckets)
