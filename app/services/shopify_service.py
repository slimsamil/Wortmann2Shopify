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
                json=product.model_dump(),
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
