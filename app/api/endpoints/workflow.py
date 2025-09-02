from fastapi import APIRouter, Depends, HTTPException
from app.services.database_service import DatabaseService
from app.services.product_service import ProductService
from app.services.shopify_service import ShopifyService
from app.services.wortmann_service import WortmannService
from app.api.deps import get_database_service, get_product_service, get_shopify_service
from app.models.product import WorkflowRequest, WorkflowResponse, SyncProductRequest, SyncProductsRequest
import time
import logging
import pyodbc
from app.core.config import settings
import httpx
import asyncio

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/sync-products-by-ids",
             response_model=WorkflowResponse,
             summary="Sync Products by IDs",
             description="Sync multiple products by their IDs. Fetches products from database, transforms to Shopify format, and updates via REST API.",
             response_description="Sync results for multiple products including success/failure counts and detailed results.")
async def sync_products_by_ids(
    request: SyncProductsRequest,
    db_service: DatabaseService = Depends(get_database_service),
    product_service: ProductService = Depends(get_product_service),
    shopify_service: ShopifyService = Depends(get_shopify_service)
):
    """
    Sync multiple products by their IDs.
    
    This endpoint:
    1. Fetches the specified products from the database
    2. Transforms them to Shopify REST API format
    3. Updates them in Shopify via PUT requests
    4. Handles both existing products (updates) and new products (creates)
    
    Parameters:
    - product_ids: List of product IDs to sync
    - dry_run: If true, only analyze changes without applying them
    - create_if_missing: If true, create products in Shopify if they don't exist
    - batch_size: Number of products to process in each batch
    
    Returns:
    - Status of the sync operation
    - Number of products processed
    - Success/failure counts
    - Detailed results for each product
    """
    start_time = time.time()
    
    try:
        logger.info(f"Starting sync for {len(request.product_ids)} products: {request.product_ids}")
        
        # Step 1: Fetch products from database
        db_products = []
        missing_products = []
        
        for product_id in request.product_ids:
            # Normalize product ID (remove 'prod-' prefix if present)
            clean_product_id = product_id.replace('prod-', '')
            product = db_service.fetch_product_by_id(clean_product_id)
            
            if product:
                db_products.append(product)
            else:
                missing_products.append(clean_product_id)
        
        if not db_products:
            raise HTTPException(status_code=404, detail=f"No products found in database. Missing: {missing_products}")
        
        if missing_products:
            logger.warning(f"Products not found in database: {missing_products}")
        
        # Step 2: Fetch images and warranties
        all_images = db_service.fetch_images()
        warranties = db_service.fetch_warranties()
        
        # Step 3: Merge and process data
        logger.info("Processing products for Shopify...")
        merged_items = product_service.merge_data(db_products, all_images, warranties)
        shopify_products = product_service.process_products(merged_items)
        
        execution_time = time.time() - start_time
        
        # Step 4: Handle dry run
        if request.dry_run:
            return WorkflowResponse(
                status="success",
                message=f"Dry run analysis: {len(shopify_products)} products ready for sync",
                total_products=len(shopify_products),
                execution_time=execution_time,
                results=[{
                    "products_ready": len(shopify_products),
                    "missing_from_db": missing_products,
                    "product_handles": [p.product.handle for p in shopify_products],
                    "sample_product": {
                        "handle": shopify_products[0].product.handle if shopify_products else None,
                        "title": shopify_products[0].product.title if shopify_products else None,
                        "price": shopify_products[0].product.variants[0].price if shopify_products and shopify_products[0].product.variants else None
                    }
                }]
            )
        
        # Step 5: Sync products in batches
        logger.info(f"Starting sync of {len(shopify_products)} products in batches of {request.batch_size}")
        
        all_results = []
        successful_uploads = 0
        failed_uploads = 0
        
        # Process in batches
        for i in range(0, len(shopify_products), request.batch_size):
            batch = shopify_products[i:i + request.batch_size]
            batch_start = time.time()
            
            logger.info(f"Processing batch {i//request.batch_size + 1}: {len(batch)} products")
            
            # Sync each product in the batch
            for shopify_product in batch:
                try:
                    # Check if product exists in Shopify
                    existing_product = await shopify_service.get_product_by_handle(shopify_product.product.handle)
                    
                    if existing_product:
                        # Update existing product
                        result = await shopify_service.update_product_by_handle(
                            shopify_product.product.handle, 
                            shopify_product.product.model_dump()
                        )
                        action = "updated"
                    else:
                        # Create new product
                        if request.create_if_missing:
                            result = await shopify_service._send_single_product(
                                httpx.AsyncClient(), shopify_product
                            )
                            action = "created"
                        else:
                            result = {"status": "skipped", "message": "Product doesn't exist and create_if_missing is false"}
                            action = "skipped"
                    
                    if result.get("status") == "success":
                        successful_uploads += 1
                    else:
                        failed_uploads += 1
                    
                    all_results.append({
                        "product_id": shopify_product.product.handle.replace("prod-", ""),
                        "handle": shopify_product.product.handle,
                        "action": action,
                        "status": result.get("status", "unknown"),
                        "message": result.get("message", ""),
                        "result": result
                    })
                    
                except Exception as e:
                    failed_uploads += 1
                    logger.error(f"Error syncing product {shopify_product.product.handle}: {str(e)}")
                    all_results.append({
                        "product_id": shopify_product.product.handle.replace("prod-", ""),
                        "handle": shopify_product.product.handle,
                        "action": "error",
                        "status": "error",
                        "message": str(e),
                        "result": {"error": str(e)}
                    })
            
            batch_time = time.time() - batch_start
            logger.info(f"Batch {i//request.batch_size + 1} completed in {batch_time:.2f}s")
            
            # Rate limiting between batches
            if i + request.batch_size < len(shopify_products):
                await asyncio.sleep(1)  # 1 second delay between batches
        
        execution_time = time.time() - start_time
        
        return WorkflowResponse(
            status="completed",
            message=f"Sync completed: {successful_uploads} successful, {failed_uploads} failed",
            total_products=len(shopify_products),
            successful_uploads=successful_uploads,
            failed_uploads=failed_uploads,
            execution_time=execution_time,
            results=[{
                "total_processed": len(shopify_products),
                "successful": successful_uploads,
                "failed": failed_uploads,
                "missing_from_db": missing_products,
                "product_results": all_results
            }]
        )
        
    except Exception as e:
        logger.error(f"Sync products by IDs failed: {str(e)}")
        raise HTTPException(status_code=500,
                          detail=f"Sync products by IDs failed: {str(e)}")


@router.post("/sync-single-product",
             response_model=WorkflowResponse,
             summary="Sync Single Product",
             description="Sync a single product by ID between database and Shopify.",
             response_description="Single product sync results including changes detected and actions taken.")
async def sync_single_product(
    request: SyncProductRequest,
    db_service: DatabaseService = Depends(get_database_service),
    product_service: ProductService = Depends(get_product_service),
    shopify_service: ShopifyService = Depends(get_shopify_service)
):
    """
    Sync a single product by ID between database and Shopify.
    
    This endpoint:
    1. Fetches the specific product from the database
    2. Checks if it exists in Shopify
    3. Compares the data and identifies changes
    4. Updates or creates the product in Shopify (unless dry_run is true)
    
    Parameters:
    - product_id: The product ID (e.g., 'eu1009805' or 'prod-eu1009805')
    - dry_run: If true, only analyze changes without applying them
    - create_if_missing: If true, create the product in Shopify if it doesn't exist
    
    Returns:
    - Status of the sync operation
    - Changes detected
    - Actions taken
    - Execution time
    """
    start_time = time.time()
    
    try:
        logger.info(f"Starting single product sync for ID: {request.product_id}")
        
        # Normalize product ID (remove 'prod-' prefix if present)
        product_id = request.product_id.replace('prod-', '')
        handle = f"prod-{product_id}"
        
        # Fetch the specific product from database
        db_product = db_service.fetch_product_by_id(product_id)
        if not db_product:
            raise HTTPException(status_code=404, detail=f"Product with ID '{product_id}' not found in database")
        
        # Fetch the product from Shopify
        shopify_product = await shopify_service.get_product_by_handle(handle)
        
        # Analyze the situation
        if shopify_product:
            # Product exists in both places - check for changes
            if shopify_service._has_changes(db_product, shopify_product):
                action = "update"
                message = f"Product {product_id} needs update in Shopify"
            else:
                action = "unchanged"
                message = f"Product {product_id} is already up to date"
        else:
            # Product doesn't exist in Shopify
            if request.create_if_missing:
                action = "create"
                message = f"Product {product_id} will be created in Shopify"
            else:
                action = "missing"
                message = f"Product {product_id} doesn't exist in Shopify and create_if_missing is false"
        
        execution_time = time.time() - start_time
        
        # If dry run, just return analysis
        if request.dry_run:
            return WorkflowResponse(
                status="success",
                message=f"Dry run analysis: {message}",
                total_products=1,
                execution_time=execution_time,
                results=[{
                    "product_id": product_id,
                    "handle": handle,
                    "action": action,
                    "db_product": db_product,
                    "shopify_product": shopify_product,
                    "changes_detected": action in ["update", "create"]
                }]
            )
        
        # Perform the actual sync
        if action == "update":
            # Update existing product
            result = await shopify_service.update_product(shopify_product['id'], db_product)
            sync_message = f"Product {product_id} updated successfully"
        elif action == "create":
            # Create new product
            # First, get images and warranties
            images = db_service.fetch_images_by_product_id(product_id)
            warranties = db_service.fetch_warranties()
            
            # Merge and process data
            merged_items = product_service.merge_data([db_product], images, warranties)
            shopify_products = product_service.process_products(merged_items)
            
            if shopify_products:
                result = await shopify_service._send_single_product(
                    httpx.AsyncClient(), shopify_products[0]
                )
                sync_message = f"Product {product_id} created successfully"
            else:
                raise HTTPException(status_code=500, detail="Failed to process product for creation")
        else:
            # No action needed
            result = {"status": "no_action", "message": "No changes needed"}
            sync_message = f"Product {product_id} - no action taken"
        
        execution_time = time.time() - start_time
        
        return WorkflowResponse(
            status="completed",
            message=sync_message,
            total_products=1,
            successful_uploads=1 if action in ["update", "create"] else 0,
            failed_uploads=0,
            execution_time=execution_time,
            results=[{
                "product_id": product_id,
                "handle": handle,
                "action": action,
                "result": result
            }]
        )
        
    except Exception as e:
        logger.error(f"Single product sync failed: {str(e)}")
        raise HTTPException(status_code=500,
                          detail=f"Single product sync failed: {str(e)}")


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
