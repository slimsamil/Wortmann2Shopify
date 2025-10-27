from fastapi import APIRouter, Depends, HTTPException
from app.services.database_service import DatabaseService
from app.services.product_service import ProductService
from app.services.shopify_service import ShopifyService
from app.api.deps import get_database_service, get_product_service, get_shopify_service
from app.models.product import WorkflowRequest, WorkflowResponse, SyncProductsRequest, DeleteProductsRequest
from typing import List, Dict, Any
import time
import logging
import httpx
import asyncio

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/upload-all-products", response_model=WorkflowResponse)
async def upload_all_products(
    request: WorkflowRequest,
    db_service: DatabaseService = Depends(get_database_service),
    product_service: ProductService = Depends(get_product_service),
    shopify_service: ShopifyService = Depends(get_shopify_service)
):
    """Uploads all Products"""
    start_time = time.time()
    
    try:
        logger.info(f"Starting Upload with params: {request.dict()}")
        
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
                message=f"Upload completed. {success_count} successful, {error_count} failed",
                total_products=len(shopify_products),
                successful_uploads=success_count,
                failed_uploads=error_count,
                execution_time=execution_time,
                results=results
            )
    
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}")
        raise HTTPException(status_code=500,
                          detail=f"Upload failed: {str(e)}")


@router.post("/delete-all-products", response_model=WorkflowResponse)
async def delete_all_products(
    shopify_service: ShopifyService = Depends(get_shopify_service)
):
    start_time = time.time()
    try:
        # Use GraphQL Bulk to efficiently fetch all product IDs/handles
        products = await shopify_service.fetch_all_product_handles_bulk()
        results = []
        deleted = 0
        failed = 0
        for p in products:
            gid = p.get('id')
            handle = p.get('handle')
            if not gid:
                results.append({"handle": handle, "status": "skipped", "message": "missing_id"})
                continue
            # Extract numeric ID from GID (gid://shopify/Product/123456789)
            try:
                sid = int(str(gid).strip().split('/')[-1])
            except Exception:
                results.append({"handle": handle, "gid": gid, "status": "skipped", "message": "invalid_gid"})
                continue
            res = await shopify_service.delete_product_by_id(sid)
            results.append({"shopify_id": sid, "handle": handle, **res})
            if res.get('status') == 'success':
                deleted += 1
            else:
                failed += 1
        execution_time = time.time() - start_time
        return WorkflowResponse(
            status="completed",
            message=f"Deleted {deleted} products; {failed} failed",
            total_products=len(products),
            successful_uploads=deleted,
            failed_uploads=failed,
            execution_time=execution_time,
            results=results
        )
    except Exception as e:
        logger.error(f"Delete all products failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Delete all products failed: {str(e)}")


@router.post("/delete-products-by-ids", response_model=WorkflowResponse)
async def delete_products_by_ids(
    request: DeleteProductsRequest,
    shopify_service: ShopifyService = Depends(get_shopify_service)
):
    start_time = time.time()
    try:
        ids = [pid.replace('prod-', '') for pid in request.product_ids]
        results = []
        for pid in ids:
            handle = f"prod-{pid}"
            product = await shopify_service.get_product_by_handle(handle)
            if product and product.get('id'):
                res = await shopify_service.delete_product_by_id(int(product['id']))
                results.append({"product_id": pid, "handle": handle, **res})
            else:
                results.append({"product_id": pid, "handle": handle, "status": "skipped", "message": "not_found"})
        execution_time = time.time() - start_time
        return WorkflowResponse(
            status="completed",
            message=f"Delete completed for {len(ids)} products",
            total_products=len(ids),
            execution_time=execution_time,
            results=results
        )
    except Exception as e:
        logger.error(f"Delete by IDs failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Delete by IDs failed: {str(e)}")


@router.post("/create-products-by-ids", response_model=WorkflowResponse)
async def create_products_by_ids(
    request: SyncProductsRequest,
    db_service: DatabaseService = Depends(get_database_service),
    product_service: ProductService = Depends(get_product_service),
    shopify_service: ShopifyService = Depends(get_shopify_service)
):
    start_time = time.time()
    try:
        ids = [pid.replace('prod-', '') for pid in request.product_ids]
        # Fetch all products in a single query
        db_products = db_service.fetch_products_by_ids(ids)
        if not db_products:
            raise HTTPException(status_code=404, detail=f"No products found in database for IDs: {ids}")
        
        # Check for missing products
        found_ids = {p.get('ProductId') for p in db_products}
        missing = [pid for pid in ids if pid not in found_ids]
        if missing:
            logger.warning(f"Some products not found in database: {missing}")

        images = db_service.fetch_images()
        warranties = db_service.fetch_warranties()
        merged = product_service.merge_data(db_products, images, warranties)
        wrappers = product_service.process_products(merged)
        results = await shopify_service.send_products_batch(wrappers, request.batch_size or 3)
        execution_time = time.time() - start_time
        return WorkflowResponse(
            status="completed",
            message=f"Create completed for {len(wrappers)} products",
            total_products=len(wrappers),
            execution_time=execution_time,
            results=results
        )
    except Exception as e:
        logger.error(f"Create by IDs failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Create by IDs failed: {str(e)}")


@router.post("/update-products-by-ids", response_model=WorkflowResponse)
async def update_products_by_ids(
    request: SyncProductsRequest,
    db_service: DatabaseService = Depends(get_database_service),
    product_service: ProductService = Depends(get_product_service),
    shopify_service: ShopifyService = Depends(get_shopify_service)
):
    start_time = time.time()
    try:
        ids = [pid.replace('prod-', '') for pid in request.product_ids]
        if not ids:
            raise HTTPException(status_code=400, detail="No product IDs provided")

        results = []
        batch_size = 200
        # Process IDs in batches to avoid overly large SQL IN clauses
        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i:i + batch_size]

            # Fetch only the current batch's products
            db_products_batch = db_service.fetch_products_by_ids(batch_ids)
            if not db_products_batch:
                # If nothing found for this batch, record skips and continue
                for pid in batch_ids:
                    results.append({"product_id": pid, "status": "skipped", "message": "not_in_db"})
                continue

            # Fetch only relevant images and warranties for this batch
            # Collect supplier_aid and garantiegruppe values from batch rows
            supplier_aids = [str(p.get('ProductId')) for p in db_products_batch if p.get('ProductId') is not None]
            groups = []
            for p in db_products_batch:
                try:
                    grp = p.get('Garantiegruppe')
                    if grp is not None:
                        groups.append(int(grp))
                except Exception:
                    continue

            images = db_service.fetch_images_by_supplier_aids(supplier_aids)
            warranties = db_service.fetch_warranties_by_groups(list(set(groups))) if groups else []

            # Create lookup for fast access within the batch (normalize keys to str)
            db_products_by_id = {str(p.get('ProductId')): p for p in db_products_batch}

            for pid in batch_ids:
                # Normalize pid to str for consistent lookup
                pid = str(pid)
                db_p = db_products_by_id.get(pid)
                if not db_p:
                    results.append({"product_id": pid, "status": "skipped", "message": "not_in_db"})
                    continue

                handle = f"prod-{pid}"
                merged = product_service.merge_data([db_p], images, warranties)
                wrappers = product_service.process_products(merged)
                if not wrappers:
                    results.append({"product_id": pid, "status": "error", "message": "transform_failed"})
                    continue

                res = await shopify_service.update_product_by_handle(handle, wrappers[0].product.model_dump())
                results.append({"product_id": pid, "handle": handle, **res})

            # Optional small pause between batches to be gentle on upstream systems
            if i + batch_size < len(ids):
                await asyncio.sleep(0.5)
        execution_time = time.time() - start_time
        return WorkflowResponse(
            status="completed",
            message=f"Update completed for {len(ids)} products",
            total_products=len(ids),
            execution_time=execution_time,
            results=results
        )
    except Exception as e:
        logger.error(f"Update by IDs failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Update by IDs failed: {str(e)}")


@router.get("/export-shopify-products",
            summary="Export All Shopify Products",
            description="Fetch all products from Shopify via GraphQL and convert to standardized JSON format for comparison",
            response_description="List of all Shopify products in standardized format")
async def export_shopify_products(
    shopify_service: ShopifyService = Depends(get_shopify_service)
) -> List[Dict[str, Any]]:
    """
    Export all Shopify products in a standardized JSON format for comparison with database products.
    
    This endpoint:
    1. Fetches all products from Shopify using GraphQL
    2. Converts them to a standardized format matching database schema
    3. Returns the list for external comparison
    
    Returns:
    - List of products in standardized format with fields like:
      - Warranty, Price_B2B_Regular, Price_B2B_Discounted, Price_B2C_inclVAT
      - Currency, VATRate, Stock, GrossWeight, NetWeight, etc.
    """
    try:
        logger.info("Starting Shopify products export via GraphQL")
        
        # Fetch all products from Shopify (use Bulk to avoid 2500 cap)
        shopify_products = await shopify_service.fetch_all_products_bulk_full()
        
        # Convert to standardized format
        standardized_products = []
        
        for product in shopify_products:
            try:
                # Parse ProductId from handle (expects prefix 'prod-')
                handle = product.get('handle') or ""
                product_id = handle.replace('prod-', '') if handle else ""
                # Title and descriptions
                title = product.get('title') or ""
                description_short = title  # fallback: use title
                long_description = product.get('body_html') or ""

                # Manufacturer and category
                manufacturer = product.get('vendor') or ""
                category = product.get('product_type') or ""
                category_path = category if category else ""

                variants = []
                if product.get('variants'):
                    if isinstance(product['variants'], list):
                        # Direct array structure (REST API or processed data)
                        variants = product['variants']
                    elif isinstance(product['variants'], dict) and 'edges' in product['variants']:
                        # GraphQL edge structure
                        variants = [edge['node'] for edge in product['variants']['edges']]

                logger.debug(f"Product {handle}: Found {len(variants)} variants")
                if variants:
                    logger.debug(f"First variant structure: {variants[0]}")

                # Warranty from variant option1 (if present)
                warranty = ""
                if product.get('variants') and len(product['variants']) > 0:
                    option1 = product['variants'][0].get('option1', '')
                    if option1:
                        warranty = option1

                # Prices from first variant
                price_b2b_regular = 0.0
                price_b2b_discounted = 0.0
                price_b2c_incl_vat = 0.0
                if product.get('variants') and len(product['variants']) > 0:
                    variant0 = product['variants'][0]
                    try:
                        price = float(variant0.get('price', 0) or 0)
                        price_b2b_regular = float(product.get('metafields', {}).get('Price_B2B_Regular', 0))
                        price_b2b_discounted = float(product.get('metafields', {}).get('Price_B2B_Discounted', 0))
                        price_b2c_incl_vat = price  # explicit per requested schema example
                    except (ValueError, TypeError):
                        pass

                # Stock from first variant
                stock = 0
                if product.get('variants') and len(product['variants']) > 0:
                    try:
                        stock = int(product['variants'][0].get('inventory_quantity', 0) or product.get('metafields', {}).get('Inventarbestand', 0) or 0)
                    except (ValueError, TypeError):
                        stock = 0

                # Weights from first variant
                gross_weight = 0.0
                net_weight = 0.0
                if product.get('variants') and len(product['variants']) > 0:
                    try:
                        w = float(product['variants'][0].get('weight', 0) or 0)
                        gross_weight = w
                        net_weight = 0.0  # unknown; keep 0 per sample
                    except (ValueError, TypeError):
                        pass

                accesssory_products = product.get('metafields', {}).get('verwandte_produkte', '')
                stock_next_delivery = product.get('metafields', {}).get('StockNextDelivery', '')
                warranty_group = product.get('metafields', {}).get('warranty_group', 0)

                # Images
                image_primary = ""
                image_additional = ""
                if product.get('images') and len(product['images']) > 0:
                    image_primary = product['images'][0].get('src', '') or ""
                    if len(product['images']) > 1:
                        image_additional = product['images'][1].get('src', '') or ""

                # Construct standardized product
                standardized_product = {
                    "ProductId": product_id,
                    "Title": title,
                    "DescriptionShort": description_short,
                    "LongDescription": long_description,
                    "Manufacturer": manufacturer,
                    "Category": category,
                    "CategoryPath": category_path,
                    "Warranty": warranty,
                    "Price_B2B_Regular": price_b2b_regular,
                    "Price_B2B_Discounted": price_b2b_discounted,
                    "Price_B2C_inclVAT": price_b2c_incl_vat,
                    "Currency": "EUR",
                    "VATRate": 19,
                    "Stock": stock,
                    "StockNextDelivery": stock_next_delivery,
                    "ImagePrimary": image_primary,
                    "ImageAdditional": image_additional,
                    "GrossWeight": gross_weight,
                    "NetWeight": net_weight,
                    "NonReturnable": False,
                    "EOL": False,
                    "Promotion": False,
                    "AccessoryProducts": accesssory_products,
                    "Garantiegruppe": warranty_group,
                }
                
                standardized_products.append(standardized_product)
                
            except Exception as e:
                logger.warning(f"Error processing product {product.get('handle', 'unknown')}: {str(e)}")
                continue
        
        logger.info(f"Successfully exported {len(standardized_products)} products from Shopify")
        return standardized_products
        
    except Exception as e:
        logger.error(f"Shopify products export failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")