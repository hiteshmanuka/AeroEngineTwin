from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Request
import io

from src.services import SimulationIngestor, SimulationIngestionError
from config import ENCODING, SIM_BUCKET

router = APIRouter()

@router.post("/ingest")
async def ingest_run(request: Request, run_id: str = Form(...), file: UploadFile = File(...)):
    raw_bytes = await file.read()  # read once, reused for both sinks below

    ingestor = SimulationIngestor()

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