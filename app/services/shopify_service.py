import httpx
import asyncio
from typing import List, Dict, Any, Optional
from app.core.config import settings
from app.models.shopify import ShopifyProductWrapper
import logging
 
logger = logging.getLogger(__name__)
 
 
class ShopifyService:
    def __init__(self):
        self.shop_url = settings.shopify_shop_url
        self.access_token = settings.shopify_access_token
        self.headers = {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': self.access_token
        }
   
    async def test_connection(self) -> bool:
        """Test Shopify API connection"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.shop_url}/admin/api/2023-10/shop.json",
                    headers={'X-Shopify-Access-Token': self.access_token}
                )
                return response.status_code == 200
        except Exception as e:
            logger.error(f"Shopify connection test failed: {str(e)}")
            return False
   
    async def fetch_all_products(self, limit: int = 250) -> List[Dict[str, Any]]:
        """Fetch all products from Shopify with pagination"""
        all_products = []
        next_page_info = None
       
        try:
            async with httpx.AsyncClient() as client:
                while True:
                    # Build URL with pagination
                    url = f"{self.shop_url}/admin/api/2023-10/products.json?limit={limit}"
                    if next_page_info:
                        url += f"&page_info={next_page_info}"
                   
                    response = await client.get(url, headers=self.headers, timeout=30.0)
                   
                    if response.status_code != 200:
                        logger.error(f"Failed to fetch products: {response.status_code} - {response.text}")
                        break
                   
                    data = response.json()
                    products = data.get('products', [])
                    all_products.extend(products)
                   
                    # Check for next page
                    link_header = response.headers.get('Link', '')
                    if 'rel="next"' in link_header:
                        # Extract next page info from Link header
                        next_page_info = link_header.split('page_info=')[1].split('>')[0]
                    else:
                        break
                   
                    # Rate limiting
                    await asyncio.sleep(0.5)
           
            logger.info(f"Fetched {len(all_products)} products from Shopify")
            return all_products
           
        except Exception as e:
            logger.error(f"Error fetching products from Shopify: {str(e)}")
            raise
   
    async def get_product_by_handle(self, handle: str) -> Optional[Dict[str, Any]]:
        """Get a specific product by handle"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.shop_url}/admin/api/2023-10/products.json?handle={handle}",
                    headers=self.headers,
                    timeout=30.0
                )
               
                if response.status_code == 200:
                    data = response.json()
                    products = data.get('products', [])
                    return products[0] if products else None
                else:
                    logger.error(f"Failed to fetch product {handle}: {response.status_code}")
                    return None
                   
        except Exception as e:
            logger.error(f"Error fetching product {handle}: {str(e)}")
            return None
   
    async def update_products_by_product_ids(self, product_ids: List[str]) -> List[Dict[str, Any]]:
        """Update multiple products in Shopify by their productIds, fetching data from database"""
        try:
            from app.services.database_service import DatabaseService
            from app.services.product_service import ProductService
           
            # Initialize services
            db_service = DatabaseService()
            product_service = ProductService()
           
            # Fetch all product data from database
            products = db_service.fetch_products()
            images = db_service.fetch_images()
            warranties = db_service.fetch_warranties()
           
            # Create lookup for products by ProductId
            products_by_id = {product.get('ProductId'): product for product in products}
           
            results = []
           
            for product_id in product_ids:
                try:
                    # Find the specific product
                    target_product = products_by_id.get(product_id)
                   
                    if not target_product:
                        results.append({
                            'status': 'error',
                            'product_id': product_id,
                            'error': f'Product with ID "{product_id}" not found in database'
                        })
                        continue
                   
                    # Merge data for this product
                    merged_items = product_service.merge_data([target_product], images, warranties)
                   
                    # Process into Shopify format
                    shopify_products = product_service.process_products(merged_items)
                   
                    if not shopify_products:
                        results.append({
                            'status': 'error',
                            'product_id': product_id,
                            'error': 'Failed to process product data for Shopify'
                        })
                        continue
                   
                    # Get the processed Shopify product data
                    shopify_product_data = shopify_products[0].product.model_dump()
                   
                    # Construct the handle format used in this system
                    handle = f"prod-{product_id}"
                   
                    # Update the product in Shopify
                    async with httpx.AsyncClient() as client:
                        response = await client.put(
                            f"{self.shop_url}/admin/api/2023-10/products.json?handle={handle}",
                            headers=self.headers,
                            json={"product": shopify_product_data},
                            timeout=30.0
                        )
                       
                        if response.status_code == 200:
                            results.append({
                                'status': 'success',
                                'product_id': product_id,
                                'handle': handle,
                                'title': shopify_product_data.get('title')
                            })
                        else:
                            results.append({
                                'status': 'error',
                                'product_id': product_id,
                                'error': response.text,
                                'status_code': response.status_code
                            })
                   
                    # Rate limiting - wait a bit between requests
                    await asyncio.sleep(0.5)
                   
                except Exception as e:
                    logger.error(f"Error updating product {product_id}: {str(e)}")
                    results.append({
                        'status': 'error',
                        'product_id': product_id,
                        'error': str(e)
                    })
           
            return results
                   
        except Exception as e:
            logger.error(f"Error in batch product update: {str(e)}")
            return [{
                'status': 'error',
                'product_id': 'batch',
                'error': str(e)
            }]
   
    async def update_product(self, shopify_id: int, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing product in Shopify"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.put(
                    f"{self.shop_url}/admin/api/2023-10/products/{shopify_id}.json",
                    headers=self.headers,
                    json={"product": product_data},
                    timeout=30.0
                )
               
                if response.status_code == 200:
                    response_data = response.json()
                    return {
                        'status': 'success',
                        'product_id': product_data.get('handle'),
                        'shopify_id': shopify_id,
                        'title': product_data.get('title')
                    }
                else:
                    return {
                        'status': 'error',
                        'product_id': product_data.get('handle'),
                        'error': response.text,
                        'status_code': response.status_code
                    }
                   
        except Exception as e:
            return {
                'status': 'error',
                'product_id': product_data.get('handle'),
                'error': str(e)
            }
   
    async def update_product_by_handle(self, handle: str, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update a product in Shopify by handle using PUT request"""
        try:
            # First get the product to get its ID
            existing_product = await self.get_product_by_handle(handle)
            if not existing_product:
                return {"status": "error", "message": f"Product with handle '{handle}' not found in Shopify"}
           
            product_id = existing_product['id']
           
            # Prepare the product data for Shopify API
            shopify_product_data = {
                "product": {
                    "title": product_data.get('title'),
                    "body_html": product_data.get('body_html'),
                    "vendor": product_data.get('vendor'),
                    "product_type": product_data.get('product_type'),
                    "tags": product_data.get('tags'),
                    "variants": product_data.get('variants', []),
                    "options": product_data.get('options', []),
                    "metafields": product_data.get('metafields', [])
                }
            }
           
            # Add images if present
            if product_data.get('images'):
                shopify_product_data["product"]["images"] = product_data['images']
           
            async with httpx.AsyncClient() as client:
                response = await client.put(
                    f"{self.shop_url}/admin/api/2023-10/products/{product_id}.json",
                    headers=self.headers,
                    json=shopify_product_data,
                    timeout=30.0
                )
               
                if response.status_code == 200:
                    updated_product = response.json()
                    logger.info(f"Successfully updated product {handle} in Shopify")
                    return {
                        "status": "success",
                        "message": f"Product {handle} updated successfully",
                        "product_id": product_id,
                        "handle": handle,
                        "data": updated_product
                    }
                else:
                    logger.error(f"Failed to update product {handle}: {response.status_code} - {response.text}")
                    return {
                        "status": "error",
                        "message": f"Failed to update product {handle}: {response.status_code}",
                        "response": response.text
                    }
                   
        except Exception as e:
            logger.error(f"Error updating product {handle}: {str(e)}")
            return {
                "status": "error",
                "message": f"Error updating product {handle}: {str(e)}"
            }
   
    async def compare_and_update_products(self, db_products: List[Dict], shopify_products: List[Dict]) -> Dict[str, Any]:
        """Compare database products with Shopify products and update changes"""
        try:
            # Create lookup dictionaries
            db_products_by_handle = {}
            for product in db_products:
                handle = f"prod-{product.get('ProductId')}"
                db_products_by_handle[handle] = product
           
            shopify_products_by_handle = {}
            for product in shopify_products:
                handle = product.get('handle')
                if handle:
                    shopify_products_by_handle[handle] = product
           
            # Find products that need updates
            products_to_update = []
            products_to_create = []
            unchanged_products = []
           
            for handle, db_product in db_products_by_handle.items():
                if handle in shopify_products_by_handle:
                    shopify_product = shopify_products_by_handle[handle]
                    if self._has_changes(db_product, shopify_product):
                        products_to_update.append({
                            'handle': handle,
                            'db_product': db_product,
                            'shopify_product': shopify_product
                        })
                    else:
                        unchanged_products.append(handle)
                else:
                    products_to_create.append({
                        'handle': handle,
                        'db_product': db_product
                    })
           
            # Find products that exist in Shopify but not in database (to be deleted or marked inactive)
            shopify_only_products = []
            for handle, shopify_product in shopify_products_by_handle.items():
                if handle not in db_products_by_handle:
                    shopify_only_products.append({
                        'handle': handle,
                        'shopify_product': shopify_product
                    })
           
            logger.info(f"Sync analysis: {len(products_to_update)} to update, {len(products_to_create)} to create, "
                       f"{len(unchanged_products)} unchanged, {len(shopify_only_products)} shopify-only")
           
            return {
                'products_to_update': products_to_update,
                'products_to_create': products_to_create,
                'unchanged_products': unchanged_products,
                'shopify_only_products': shopify_only_products
            }
           
        except Exception as e:
            logger.error(f"Error comparing products: {str(e)}")
            raise
   
    def _has_changes(self, db_product: Dict, shopify_product: Dict) -> bool:
        """Check if there are changes between database and Shopify product"""
        try:
            # Compare key fields
            db_title = db_product.get('Title', '')
            shopify_title = shopify_product.get('title', '')
           
            db_price = float(db_product.get('Price_B2C_inclVAT', 0))
            shopify_price = float(shopify_product.get('variants', [{}])[0].get('price', 0))
           
            db_stock = int(db_product.get('Stock', 0))
            shopify_stock = int(shopify_product.get('variants', [{}])[0].get('inventory_quantity', 0))
           
            db_description = db_product.get('LongDescription', '')
            shopify_description = shopify_product.get('body_html', '')
           
            # Check for changes
            if (db_title != shopify_title or
                abs(db_price - shopify_price) > 0.01 or  # Allow small floating point differences
                db_stock != shopify_stock or
                db_description != shopify_description):
                return True
           
            return False
           
        except Exception as e:
            logger.error(f"Error comparing product {db_product.get('ProductId')}: {str(e)}")
            return True  # Assume there are changes if comparison fails
   
    async def send_products_batch(self, products: List[ShopifyProductWrapper], batch_size: int = 2) -> List[Dict[str, Any]]:
        """Send products to Shopify in batches"""
        results = []
        # Ensure batch_size is a valid positive integer
        try:
            batch_size = int(batch_size)
        except (TypeError, ValueError):
            batch_size = None
        if not batch_size or batch_size <= 0:
            batch_size = len(products) if len(products) > 0 else 1
        async with httpx.AsyncClient() as client:
            for i in range(0, len(products), batch_size):
                batch = products[i:i + batch_size]
               
                tasks = []
                for product in batch:
                    task = self._send_single_product(client, product)
                    tasks.append(task)
               
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                results.extend(batch_results)
               
                # Rate limiting - wait 1 second between batches
                if i + batch_size < len(products):
                    await asyncio.sleep(1)
       
        return results
   
    async def _send_single_product(self, client: httpx.AsyncClient, product: ShopifyProductWrapper ) -> Dict[str, Any]:
        """Send a single product to Shopify"""
        try:
            response = await client.post(
                f"{self.shop_url}/admin/api/2023-10/products.json",
                headers=self.headers,
                json={"product": product.product.model_dump()},
                timeout=30.0
            )
           
            if response.status_code == 201:
                response_data = response.json()
                return {
                    'status': 'success',
                    'product_id': product.product.handle,
                    'shopify_id': response_data.get('product', {}).get('id'),
                    'title': product.product.title
                }
            else:
                return {
                    'status': 'error',
                    'product_id': product.product.handle,
                    'error': response.text,
                    'status_code': response.status_code
                }
        except Exception as e:
            return {
                'status': 'error',
                'product_id': product.product.handle,
                'error': str(e)
            }
 
 
shopify_service = ShopifyService()
 