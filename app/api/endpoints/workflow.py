from fastapi import APIRouter, Depends, HTTPException
from app.models.product import WorkflowRequest, WorkflowResponse
from app.services.database_service import DatabaseService
from app.services.product_service import ProductService
from app.services.shopify_service import ShopifyService
from app.api.deps import get_database_service, get_product_service, get_shopify_service
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
        raise HTTPException(status_code=500, detail=f"Workflow execution failed: {str(e)}")


@router.post("/test-workflow", response_model=WorkflowResponse)
async def test_workflow(
    request: WorkflowRequest,
    db_service: DatabaseService = Depends(get_database_service),
    product_service: ProductService = Depends(get_product_service),
    shopify_service: ShopifyService = Depends(get_shopify_service)
):
    """Test the workflow with only the first 10 products"""
    start_time = time.time()
    
    try:
        logger.info(f"Starting test workflow execution with params: {request.dict()}")
        
        # Step 1: Fetch data from databases (limit to 10 products)
        logger.info("Fetching first 10 products from databases...")
        products = db_service.fetch_products(10)
        images = db_service.fetch_images(10)
        warranties = db_service.fetch_warranties(10)
        
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
                message="Test dry run completed successfully",
                total_products=len(shopify_products),
                execution_time=execution_time,
                results=[{
                    "sample_product_count": len(shopify_products),
                    "sample_product_handle": shopify_products[0].product.handle if shopify_products else None
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
        raise HTTPException(status_code=500, detail=f"Test workflow execution failed: {str(e)}")


@router.post("/sync-products", response_model=WorkflowResponse)
async def sync_products(
    request: WorkflowRequest,
    db_service: DatabaseService = Depends(get_database_service),
    product_service: ProductService = Depends(get_product_service),
    shopify_service: ShopifyService = Depends(get_shopify_service)
):
    """Sync products between database and Shopify - detect and update changes"""
    start_time = time.time()
    
    try:
        logger.info(f"Starting product sync with params: {request.dict()}")
        
        # Step 1: Fetch data from databases
        logger.info("Fetching data from databases...")
        products = db_service.fetch_products()
        images = db_service.fetch_images()
        warranties = db_service.fetch_warranties()
        
        if not products:
            raise HTTPException(status_code=404, detail="No products found in database")
        
        # Step 2: Fetch all products from Shopify
        logger.info("Fetching products from Shopify...")
        shopify_products = await shopify_service.fetch_all_products()
        
        # Step 3: Compare and analyze changes
        logger.info("Comparing database and Shopify products...")
        sync_analysis = await shopify_service.compare_and_update_products(products, shopify_products)
        
        execution_time = time.time() - start_time
        
        # Step 4: Process results
        if request.dry_run:
            return WorkflowResponse(
                status="success",
                message="Sync analysis completed (dry run)",
                total_products=len(products),
                execution_time=execution_time,
                results=[{
                    "sync_analysis": sync_analysis,
                    "summary": {
                        "total_db_products": len(products),
                        "total_shopify_products": len(shopify_products),
                        "products_to_update": len(sync_analysis['products_to_update']),
                        "products_to_create": len(sync_analysis['products_to_create']),
                        "unchanged_products": len(sync_analysis['unchanged_products']),
                        "shopify_only_products": len(sync_analysis['shopify_only_products'])
                    }
                }]
            )
        else:
            # Step 5: Update changed products
            logger.info(f"Updating {len(sync_analysis['products_to_update'])} changed products...")
            update_results = []
            
            for item in sync_analysis['products_to_update']:
                handle = item['handle']
                db_product = item['db_product']
                shopify_product = item['shopify_product']
                
                # Process the database product into Shopify format
                merged_items = product_service.merge_data([db_product], images, warranties)
                shopify_products_list = product_service.process_products(merged_items)
                
                if shopify_products_list:
                    shopify_product_data = shopify_products_list[0].product.model_dump()
                    shopify_id = shopify_product.get('id')
                    
                    if shopify_id:
                        result = await shopify_service.update_product(shopify_id, shopify_product_data)
                        update_results.append(result)
                    else:
                        update_results.append({
                            'status': 'error',
                            'product_id': handle,
                            'error': 'Shopify product ID not found'
                        })
            
            # Step 6: Create new products
            logger.info(f"Creating {len(sync_analysis['products_to_create'])} new products...")
            create_results = []
            
            for item in sync_analysis['products_to_create']:
                db_product = item['db_product']
                
                # Process the database product into Shopify format
                merged_items = product_service.merge_data([db_product], images, warranties)
                shopify_products_list = product_service.process_products(merged_items)
                
                if shopify_products_list:
                    create_results.extend(await shopify_service.send_products_batch(shopify_products_list, 1))
            
            # Combine results
            all_results = update_results + create_results
            success_count = sum(1 for r in all_results if isinstance(r, dict) and r.get('status') == 'success')
            error_count = len(all_results) - success_count
            
            execution_time = time.time() - start_time
            
            return WorkflowResponse(
                status="completed",
                message=f"Sync completed. {success_count} successful, {error_count} failed",
                total_products=len(products),
                successful_uploads=success_count,
                failed_uploads=error_count,
                execution_time=execution_time,
                results=[{
                    "sync_analysis": sync_analysis,
                    "update_results": update_results,
                    "create_results": create_results,
                    "summary": {
                        "total_db_products": len(products),
                        "total_shopify_products": len(shopify_products),
                        "products_updated": len(update_results),
                        "products_created": len(create_results),
                        "unchanged_products": len(sync_analysis['unchanged_products']),
                        "shopify_only_products": len(sync_analysis['shopify_only_products'])
                    }
                }]
            )
    
    except Exception as e:
        logger.error(f"Product sync failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Product sync failed: {str(e)}")
