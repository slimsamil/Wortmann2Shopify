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


@router.post("/enrich-rental-products", response_model=WorkflowResponse)
async def enrich_rental_products(
    db_service: DatabaseService = Depends(get_database_service),
):
    """
    Enrich C12/C24/C36 rental products with data from their main products.
    Updates LongDescription, ImagePrimary, ImageAdditional, and AccessoryProducts
    from the corresponding main product (without C12/C24/C36 suffix).
    """
    start_time = time.time()
    try:
        logger.info("Starting rental products enrichment...")
        
        # Call the enrichment function
        enriched_count = db_service.enrich_rental_products_with_main_product_data()
        
        execution_time = time.time() - start_time
        
        return WorkflowResponse(
            status="completed",
            message=f"Successfully enriched {enriched_count} rental products with main product data",
            total_products=enriched_count,
            successful_uploads=enriched_count,
            failed_uploads=0,
            execution_time=execution_time,
            results=[{
                "enriched_products": enriched_count,
                "message": "Rental products (C12/C24/C36) have been enriched with data from their main products"
            }]
        )
        
    except Exception as e:
        logger.error(f"Rental products enrichment failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Rental products enrichment failed: {str(e)}")


@router.get("/rental-products-status")
async def get_rental_products_status(
    db_service: DatabaseService = Depends(get_database_service),
):
    """
    Get status of rental products (C12/C24/C36) and their enrichment status.
    Shows which products need enrichment with main product data.
    """
    try:
        logger.info("Getting rental products status...")
        
        # Get the status information
        status = db_service.get_rental_products_status()
        
        return {
            "status": "success",
            "message": f"Found {status['total_rental_products']} rental products",
            "summary": {
                "total_rental_products": status['total_rental_products'],
                "missing_long_description": status['missing_long_description'],
                "missing_images": status['missing_images'],
                "missing_accessories": status['missing_accessories']
            },
            "products": status['products']
        }
        
    except Exception as e:
        logger.error(f"Failed to get rental products status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get rental products status: {str(e)}")