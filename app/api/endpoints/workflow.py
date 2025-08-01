from fastapi import APIRouter, Depends, HTTPException
from app.services.database_service import DatabaseService
from app.services.product_service import ProductService
from app.services.shopify_service import ShopifyService
from app.api.deps import get_database_service, get_product_service, get_shopify_service
from app.models.product import WorkflowRequest, WorkflowResponse
import time
import logging

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/execute-workflow", response_model=WorkflowResponse)
async def execute_workflow(
    request: WorkflowRequest,
    db_service: DatabaseService = Depends(get_database_service),
    product_service: ProductService = Depends(get_product_service),
    shopify_service: ShopifyService = Depends(get_shopify_service)
):
    """Execute the complete workflow"""
    start_time = time.time()
    
    try:
        logger.info(f"Starting workflow execution with params: {request.dict()}")
        
        # Step 1: Fetch data from databases
        logger.info("Fetching data from databases...")
        products = db_service.fetch_products()
        images = db_service.fetch_images()
        warranties = db_service.fetch_warranties()
        
        if not products:
            raise HTTPException(status_code=404, detail="No products found in database")
        
        # Step 2: Merge data
        logger.info("Merging data...")
        merged_items = product_service.merge_data(products, images, warranties)
        
        # Step 3: Process into Shopify format
        logger.info("Processing products for Shopify...")
        shopify_products = product_service.process_products(merged_items)
        
        execution_time = time.time() - start_time
        
        # Step 4: Send to Shopify (unless dry run)
        if request.dry_run:
            return WorkflowResponse(
                status="success",
                message="Dry run completed successfully",
                total_products=len(shopify_products),
                execution_time=execution_time,
                results=[{
                    "sample_product_count": len(shopify_products),
                    "sample_product_handle": shopify_products[0].product.handle if shopify_products else None
                }]
            )
        else:
            logger.info(f"Sending {len(shopify_products)} products to Shopify...")
            results = await shopify_service.send_products_batch(shopify_products, request.batch_size)
            
            success_count = sum(1 for r in results if isinstance(r, dict) and r.get('status') == 'success')
            error_count = len(results) - success_count
            
            execution_time = time.time() - start_time
            
            return WorkflowResponse(
                status="completed",
                message=f"Workflow completed. {success_count} successful, {error_count} failed",
                total_products=len(shopify_products),
                successful_uploads=success_count,
                failed_uploads=error_count,
                execution_time=execution_time,
                results=results
            )
    
    except Exception as e:
        logger.error(f"Workflow execution failed: {str(e)}")
        raise HTTPException(status_code=500,
                          detail=f"Workflow execution failed: {str(e)}")


@router.post("/test-workflow", response_model=WorkflowResponse)
async def test_workflow(
    request: WorkflowRequest,
    db_service: DatabaseService = Depends(get_database_service),
    product_service: ProductService = Depends(get_product_service),
    shopify_service: ShopifyService = Depends(get_shopify_service)
):
    """Execute workflow with only test data (products with TEST prefix)"""
    start_time = time.time()
    
    try:
        logger.info(f"Starting test workflow execution with params: {request.dict()}")
        
        # Step 1: Fetch only test data from databases
        logger.info("Fetching test data from databases...")
        products = db_service.fetch_test_products(limit=request.product_limit or 10)
        images = db_service.fetch_test_images(limit=request.image_limit or 10)
        warranties = db_service.fetch_warranties()
        
        if not products:
            raise HTTPException(status_code=404, detail="No test products found in database. Run the test data script first.")
        
        # Step 2: Merge data
        logger.info("Merging test data...")
        merged_items = product_service.merge_data(products, images, warranties)
        
        # Step 3: Process into Shopify format
        logger.info("Processing test products for Shopify...")
        shopify_products = product_service.process_products(merged_items)
        
        execution_time = time.time() - start_time
        
        # Step 4: Send to Shopify (unless dry run)
        if request.dry_run:
            return WorkflowResponse(
                status="success",
                message="Test workflow dry run completed successfully",
                total_products=len(shopify_products),
                execution_time=execution_time,
                results=[{
                    "test_products": len(shopify_products),
                    "sample_product_handle": shopify_products[0].product.handle if shopify_products else None,
                    "product_ids": [p.product.handle for p in shopify_products[:5]]  # Show first 5 handles
                }]
            )
        else:
            logger.info(f"Sending {len(shopify_products)} test products to Shopify...")
            results = await shopify_service.send_products_batch(shopify_products, request.batch_size)
            
            success_count = sum(1 for r in results if isinstance(r, dict) and r.get('status') == 'success')
            error_count = len(results) - success_count
            
            execution_time = time.time() - start_time
            
            return WorkflowResponse(
                status="completed",
                message=f"Test workflow completed. {success_count} successful, {error_count} failed",
                total_products=len(shopify_products),
                successful_uploads=success_count,
                failed_uploads=error_count,
                execution_time=execution_time,
                results=results
            )
    
    except Exception as e:
        logger.error(f"Test workflow execution failed: {str(e)}")
        raise HTTPException(status_code=500,
                          detail=f"Test workflow execution failed: {str(e)}")


@router.post("/sync-products", response_model=WorkflowResponse)
async def sync_products(
    request: WorkflowRequest,
    db_service: DatabaseService = Depends(get_database_service),
    product_service: ProductService = Depends(get_product_service),
    shopify_service: ShopifyService = Depends(get_shopify_service)
):
    """Sync products between database and Shopify"""
    start_time = time.time()
    
    try:
        logger.info(f"Starting product sync with params: {request.dict()}")
        
        # Fetch data from both sources
        db_products = db_service.fetch_products()
        shopify_products = await shopify_service.fetch_all_products()
        
        if not db_products:
            raise HTTPException(status_code=404, detail="No products found in database")
        
        # Compare and update
        sync_results = await shopify_service.compare_and_update_products(db_products, shopify_products)
        
        execution_time = time.time() - start_time
        
        return WorkflowResponse(
            status="completed",
            message="Product sync completed",
            total_products=len(db_products),
            execution_time=execution_time,
            results=[sync_results]
        )
    
    except Exception as e:
        logger.error(f"Product sync failed: {str(e)}")
        raise HTTPException(status_code=500,
                          detail=f"Product sync failed: {str(e)}")
