from fastapi import APIRouter, UploadFile, File, Form, HTTPException, Request

from src.services import SimulationIngestor, SimulationIngestionError
from config import ENCODING

router = APIRouter()

@router.post("/ingest")
async def ingest_run(request: Request, run_id: str = Form(...), file: UploadFile = File(...)):
    ingestor = SimulationIngestor(encoding=ENCODING)
    try:
        row_count = await ingestor.ingest_to_db(run_id, file.file, request.app.state.db_pool)
    except SimulationIngestionError as e:
        raise HTTPException(status_code=422, detail=str(e))
    return {"run_id": run_id, "rows_ingested": row_count}