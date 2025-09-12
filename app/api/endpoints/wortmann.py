from fastapi import APIRouter, Depends, HTTPException
from app.services.database_service import DatabaseService
from app.services.wortmann_service import WortmannService
from app.api.deps import get_database_service
from app.models.product import WorkflowResponse
import time
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

@router.post("/wortmann-import", response_model=WorkflowResponse)
async def wortmann_import(
    db_service: DatabaseService = Depends(get_database_service),
):
    start_time = time.time()
    try:
        service = WortmannService(db_service)
        result = service.run_import()
        execution_time = time.time() - start_time
        return WorkflowResponse(
            status="completed",
            message="Wortmann import finished",
            total_products=result.get('products_processed', 0),
            successful_uploads=result.get('products_upserted', 0),
            failed_uploads=0,
            execution_time=execution_time,
            results=[result]
        )
    except Exception as e:
        logger.error(f"Wortmann import failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Wortmann import failed: {str(e)}")