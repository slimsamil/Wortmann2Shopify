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
        """Fetch all products using GraphQL pagination and return REST-like dicts.
        The "limit" parameter is retained for compatibility but GraphQL will paginate at 250/page.
        """
        async def _gid_to_numeric_id(gid: Optional[str]) -> Optional[int]:
            if not gid or not isinstance(gid, str):
                return None
            # gid format: gid://shopify/Product/1234567890
            try:
                return int(gid.rsplit('/', 1)[-1])
            except Exception:
                return None

        def _map_product(node: Dict[str, Any]) -> Dict[str, Any]:
            # Map GraphQL product node to REST-like product dict consumed by downstream code
            mapped: Dict[str, Any] = {
                "id": _gid_to_numeric_id(node.get("id")),
                "handle": node.get("handle"),
                "title": node.get("title"),
                "body_html": node.get("bodyHtml"),
                "vendor": node.get("vendor"),
                "product_type": node.get("productType"),
                "tags": node.get("tags") or [],
            }
            # Options
            options_out: List[Dict[str, Any]] = []
            for opt in (node.get("options") or []):
                options_out.append({"name": opt.get("name"), "values": opt.get("values") or []})
            if options_out:
                mapped["options"] = options_out

            # Variants
            variants_out: List[Dict[str, Any]] = []
            for v in (node.get("variants", {}).get("nodes") or []):
                # selectedOptions -> option1/2/3
                option_names = [so.get("value") for so in (v.get("selectedOptions") or [])]
                variants_out.append({
                    "price": v.get("price"),
                    "sku": v.get("sku"),
                    "inventory_quantity": v.get("inventoryQuantity"),
                    "inventory_management": v.get("inventoryManagement"),
                    "inventory_policy": v.get("inventoryPolicy"),
                    "weight": v.get("weight"),
                    "weight_unit": v.get("weightUnit"),
                    "option1": option_names[0] if len(option_names) > 0 else None,
                    "option2": option_names[1] if len(option_names) > 1 else None,
                    "option3": option_names[2] if len(option_names) > 2 else None,
                })
            if variants_out:
                mapped["variants"] = variants_out

            # Images
            images_out: List[Dict[str, Any]] = []
            for img in (node.get("images", {}).get("nodes") or []):
                src = img.get("url") or img.get("src")
                if src:
                    images_out.append({"src": src})
            if images_out:
                mapped["images"] = images_out

            return mapped

        query = (
            "query($first:Int!,$after:String) { "
            "  products(first: $first, after: $after) { "
            "    pageInfo { hasNextPage endCursor } "
            "    edges { "
            "      cursor "
            "      node { "
            "        id handle title bodyHtml vendor productType tags "
            "        options { name values } "
            "        variants(first: 100) { nodes { "
            "          price sku inventoryQuantity inventoryManagement inventoryPolicy weight weightUnit "
            "          selectedOptions { name value } "
            "        } } "
            "        images(first: 100) { nodes { url } } "
            "      } "
            "    } "
            "  } "
            "}"
        )

        all_products: List[Dict[str, Any]] = []
        after: Optional[str] = None
        page_size = min(max(1, limit), 250)
        try:
            async with httpx.AsyncClient() as client:
                while True:
                    # Throttle-aware request with retries
                    max_retries = 6
                    backoff = 0.5
                    for attempt in range(max_retries):
                        resp = await self._graphql(client, query, {"first": page_size, "after": after})
                        if resp.status_code == 200:
                            break
                        if resp.status_code in (429, 500, 502, 503, 504):
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 2, 8)
                            continue
                        logger.error(f"GraphQL products fetch failed: {resp.status_code} - {resp.text}")
                        return all_products

                    if resp.status_code != 200:
                        logger.error(f"GraphQL products fetch failed after retries: {resp.status_code}")
                        return all_products

                    data = resp.json() or {}
                    # Optional: respect throttle status
                    try:
                        throttle = ((data.get("extensions") or {}).get("cost") or {}).get("throttleStatus") or {}
                        remaining = throttle.get("currentlyAvailable")
                        restore_rate = throttle.get("restoreRate") or 1
                        if isinstance(remaining, int) and remaining < 10:
                            await asyncio.sleep(max(1, int(10 / max(restore_rate, 1))))
                    except Exception:
                        pass

                    edges = (((data.get("data") or {}).get("products") or {}).get("edges") or [])
                    for edge in edges:
                        node = (edge or {}).get("node") or {}
                        all_products.append(_map_product(node))
                    page_info = (((data.get("data") or {}).get("products") or {}).get("pageInfo") or {})
                    if page_info.get("hasNextPage"):
                        after = page_info.get("endCursor")
                        await asyncio.sleep(0.1)
                        continue
                    break
            logger.info(f"Fetched {len(all_products)} products from Shopify via GraphQL")
            return all_products
        except Exception as e:
            logger.error(f"Error fetching products from Shopify (GraphQL): {str(e)}")
            raise
   
    async def get_product_by_handle(self, handle: str) -> Optional[Dict[str, Any]]:
        """Get a specific product by handle via GraphQL and map to REST-like struct."""
        def _gid_to_numeric_id(gid: Optional[str]) -> Optional[int]:
            if not gid or not isinstance(gid, str):
                return None
            try:
                return int(gid.rsplit('/', 1)[-1])
            except Exception:
                return None

        def _map(node: Dict[str, Any]) -> Dict[str, Any]:
            out = {
                "id": _gid_to_numeric_id(node.get("id")),
                "handle": node.get("handle"),
                "title": node.get("title"),
                "body_html": node.get("bodyHtml"),
                "vendor": node.get("vendor"),
                "product_type": node.get("productType"),
                "tags": node.get("tags") or [],
            }
            # Minimal variants for callers that read price/inventory
            variants_nodes = (node.get("variants", {}).get("nodes") or [])
            variants_out: List[Dict[str, Any]] = []
            for v in variants_nodes:
                option_names = [so.get("value") for so in (v.get("selectedOptions") or [])]
                variants_out.append({
                    "price": v.get("price"),
                    "sku": v.get("sku"),
                    "inventory_quantity": v.get("inventoryQuantity"),
                    "inventory_management": v.get("inventoryManagement"),
                    "inventory_policy": v.get("inventoryPolicy"),
                    "weight": v.get("weight"),
                    "weight_unit": v.get("weightUnit"),
                    "option1": option_names[0] if len(option_names) > 0 else None,
                })
            if variants_out:
                out["variants"] = variants_out
            return out

        query = (
            "query($handle:String!) { "
            "  productByHandle(handle: $handle) { "
            "    id handle title bodyHtml vendor productType tags "
            "    variants(first: 25) { nodes { "
            "      price sku inventoryQuantity inventoryManagement inventoryPolicy weight weightUnit "
            "      selectedOptions { name value } "
            "    } } "
            "  } "
            "}"
        )
        try:
            async with httpx.AsyncClient() as client:
                resp = await self._graphql(client, query, {"handle": handle})
                if resp.status_code != 200:
                    logger.error(f"Failed to fetch product {handle} (GraphQL): {resp.status_code} - {resp.text}")
                    return None
                data = resp.json() or {}
                node = ((data.get("data") or {}).get("productByHandle") or None)
                if not node:
                    return None
                return _map(node)
        except Exception as e:
            logger.error(f"Error fetching product {handle} (GraphQL): {str(e)}")
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

    async def delete_product_by_id(self, product_id: int) -> Dict[str, Any]:
        """Delete a Shopify product by its numeric Shopify ID."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.delete(
                    f"{self.shop_url}/admin/api/2023-10/products/{product_id}.json",
                    headers=self.headers,
                    timeout=30.0
                )
                if response.status_code in (200, 204):
                    return {"status": "success", "shopify_id": product_id}
                return {"status": "error", "shopify_id": product_id, "response": response.text, "code": response.status_code}
        except Exception as e:
            logger.error(f"Error deleting Shopify product {product_id}: {str(e)}")
            return {"status": "error", "shopify_id": product_id, "error": str(e)}

    async def sync_with_database(self, db_products: List[Dict]) -> Dict[str, Any]:
        """Ensure Shopify matches database: create missing, update changed, delete extraneous.
        Efficiently compares ID sets and performs minimal operations.
        """
        # Build DB id -> product map
        db_by_handle: Dict[str, Dict] = {}
        for p in db_products:
            pid = p.get('ProductId')
            if not pid:
                continue
            db_by_handle[f"prod-{pid}"] = p

        # Fetch shopify products (handles and ids)
        all_shopify = await self.fetch_all_products()
        shopify_by_handle: Dict[str, Dict[str, Any]] = {}
        for sp in all_shopify:
            h = sp.get('handle')
            if h:
                shopify_by_handle[h] = sp

        db_handles = set(db_by_handle.keys())
        sh_handles = set(shopify_by_handle.keys())

        to_create = db_handles - sh_handles
        to_delete = sh_handles - db_handles
        to_consider_update = db_handles & sh_handles

        from app.services.product_service import ProductService
        from app.services.database_service import DatabaseService
        
        product_service = ProductService()
        db_service = DatabaseService()
        
        # Fetch images and warranties once for all transforms
        try:
            images = db_service.fetch_images()
        except Exception:
            images = []
        try:
            warranties = db_service.fetch_warranties()
        except Exception:
            warranties = []

        # Precompute expected Shopify payloads and wrappers for all DB handles
        expected_by_handle: Dict[str, Dict[str, Any]] = {}
        wrapper_by_handle: Dict[str, ShopifyProductWrapper] = {}
        for handle in db_handles:
            try:
                merged_items = product_service.merge_data([db_by_handle[handle]], images, warranties)
                wrappers = product_service.process_products(merged_items)
                if wrappers:
                    wrapper_by_handle[handle] = wrappers[0]
                    expected_by_handle[handle] = wrappers[0].product.model_dump()
            except Exception:
                continue

        # Lightweight normalizer to compare payloads order-insensitively
        def _normalize_rest_like(product: Dict[str, Any]) -> Dict[str, Any]:
            norm: Dict[str, Any] = {}
            norm["title"] = product.get("title")
            norm["body_html"] = product.get("body_html")
            norm["vendor"] = product.get("vendor")
            norm["product_type"] = product.get("product_type")
            tags = product.get("tags") or []
            if isinstance(tags, str):
                tags = [t.strip() for t in tags.split(",") if t.strip()]
            norm["tags"] = sorted(tags)
            options_out = []
            for o in product.get("options", []) or []:
                options_out.append({
                    "name": o.get("name"),
                    "values": sorted((o.get("values") or []))
                })
            options_out.sort(key=lambda x: (x.get("name") or ""))
            norm["options"] = options_out
            variants_out = []
            for v in product.get("variants", []) or []:
                variants_out.append({
                    "price": float(v.get("price", 0) or 0),
                    "sku": v.get("sku"),
                    "inventory_quantity": int(v.get("inventory_quantity", 0) or 0),
                    "inventory_management": v.get("inventory_management"),
                    "inventory_policy": v.get("inventory_policy"),
                    "weight": float(v.get("weight", 0) or 0),
                    "weight_unit": v.get("weight_unit"),
                    "option1": v.get("option1"),
                    "option2": v.get("option2"),
                    "option3": v.get("option3"),
                })
            variants_out.sort(key=lambda x: (x.get("sku") or "", x.get("option1") or ""))
            norm["variants"] = variants_out
            images_out = []
            for img in product.get("images", []) or []:
                src = img.get("src")
                if src:
                    images_out.append(src)
            images_out.sort()
            norm["images"] = images_out
            return norm

        # Compute actual updates count using precomputed expected payloads
        updates_count = 0
        for handle in to_consider_update:
            expected = expected_by_handle.get(handle)
            actual = shopify_by_handle.get(handle)
            if expected and actual:
                if _normalize_rest_like(expected) != _normalize_rest_like(actual):
                    updates_count += 1
        logger.info(f"Sync changes: create={len(to_create)}, update={updates_count}, delete={len(to_delete)}")

        results: Dict[str, Any] = {"created": [], "updated": [], "deleted": [], "skipped": []}

        # 1) Create (reuse precomputed wrappers)
        create_wrappers: List[ShopifyProductWrapper] = []  # type: ignore[name-defined]
        for handle in to_create:
            w = wrapper_by_handle.get(handle)
            if w:
                create_wrappers.append(w)
        if create_wrappers:
            batch_results = await self.send_products_batch(create_wrappers, batch_size=3)
            results["created"].extend(batch_results)

        # 2) Update (only if changed) using precomputed expected payloads
        for handle in to_consider_update:
            expected = expected_by_handle.get(handle)
            actual = shopify_by_handle.get(handle)
            if not expected or not actual:
                continue
            if _normalize_rest_like(expected) != _normalize_rest_like(actual):
                upd = await self.update_product_by_handle(handle, expected)
                results["updated"].append(upd)
            else:
                results["skipped"].append({"handle": handle, "reason": "no_changes"})

        # 3) Delete extras
        for handle in to_delete:
            sp = shopify_by_handle[handle]
            sid = sp.get('id')
            if sid:
                del_res = await self.delete_product_by_id(int(sid))
                results["deleted"].append(del_res)

        return results
   
    def _has_changes(self, db_product: Dict, shopify_product: Dict) -> bool:
        """Deep-compare DB product (transformed) vs current Shopify product.
        Compares all relevant fields (title, body_html, vendor, product_type, tags,
        options, variants, images). Ignores only fields we don't track (e.g., StockNextDelivery).
        """
        try:
            from app.services.database_service import DatabaseService
            from app.services.product_service import ProductService

            # 1) Build the expected Shopify payload from DB using our standard transform
            db_service = DatabaseService()
            product_service = ProductService()
            try:
                images = db_service.fetch_images()
            except Exception:
                images = []
            try:
                warranties = db_service.fetch_warranties()
            except Exception:
                warranties = []

            merged_items = product_service.merge_data([db_product], images, warranties)
            wrappers = product_service.process_products(merged_items)
            if not wrappers:
                # If we can't transform, be conservative and report changes
                return True
            expected_payload = wrappers[0].product.model_dump()

            # 2) Normalize both expected and actual for a fair, order-insensitive comparison
            def _normalize(product: Dict[str, Any]) -> Dict[str, Any]:
                norm: Dict[str, Any] = {}
                # Core fields
                norm["title"] = product.get("title")
                norm["body_html"] = product.get("body_html")
                norm["vendor"] = product.get("vendor")
                norm["product_type"] = product.get("product_type")
                # Tags: as sorted list
                tags = product.get("tags") or []
                if isinstance(tags, str):
                    tags = [t.strip() for t in tags.split(",") if t.strip()]
                norm["tags"] = sorted(tags)
                # Options
                options_out = []
                for o in product.get("options", []) or []:
                    options_out.append({
                        "name": o.get("name"),
                        "values": sorted((o.get("values") or []))
                    })
                options_out.sort(key=lambda x: (x.get("name") or ""))
                norm["options"] = options_out
                # Variants (only compare fields we manage)
                variants_out = []
                for v in product.get("variants", []) or []:
                    variants_out.append({
                        "price": float(v.get("price", 0) or 0),
                        "sku": v.get("sku"),
                        "inventory_quantity": int(v.get("inventory_quantity", 0) or 0),
                        "inventory_management": v.get("inventory_management"),
                        "inventory_policy": v.get("inventory_policy"),
                        "weight": float(v.get("weight", 0) or 0),
                        "weight_unit": v.get("weight_unit"),
                        "option1": v.get("option1"),
                        "option2": v.get("option2"),
                        "option3": v.get("option3"),
                    })
                variants_out.sort(key=lambda x: (x.get("sku") or "", x.get("option1") or ""))
                norm["variants"] = variants_out
                # Images -> list of src strings
                images_out = []
                for img in product.get("images", []) or []:
                    src = img.get("src")
                    if src:
                        images_out.append(src)
                images_out.sort()
                norm["images"] = images_out
                return norm

            expected_norm = _normalize(expected_payload)

            # The shopify_product coming from REST list has our desired keys already
            actual_norm = _normalize(shopify_product)

            # 3) Compare deeply
            return expected_norm != actual_norm

        except Exception as e:
            logger.error(f"Error comparing product {db_product.get('ProductId')}: {str(e)}")
            # Be safe: if comparison fails, assume changes exist to avoid missing updates
            return True
   
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


    # ----------------- GraphQL Bulk Operations -----------------
    async def _graphql(self, client: httpx.AsyncClient, query: str, variables: Optional[Dict[str, Any]] = None) -> httpx.Response:
        payload = {"query": query}
        if variables is not None:
            payload["variables"] = variables
        return await client.post(
            f"{self.shop_url}/admin/api/2023-10/graphql.json",
            headers={
                'Content-Type': 'application/json',
                'X-Shopify-Access-Token': self.access_token
            },
            json=payload,
            timeout=60.0
        )

    async def start_products_bulk_handles(self) -> Dict[str, Any]:
        """Start a GraphQL Bulk Operation to export all products with id, handle, updatedAt."""
        bulk_query = (
            "{ products { edges { node { id handle updatedAt } } } }"
        )
        mutation = (
            "mutation bulkOperationRunQuery($query: String!) { "
            "  bulkOperationRunQuery(query: $query) { "
            "    bulkOperation { id status } "
            "    userErrors { field message } "
            "  } "
            "}"
        )
        async with httpx.AsyncClient() as client:
            resp = await self._graphql(client, mutation, {"query": bulk_query})
            data = resp.json()
            return data

    async def start_products_bulk_full(self) -> Dict[str, Any]:
        """Start a GraphQL Bulk Operation to export all products with full known fields."""
        bulk_query = (
            "{ products { edges { node { "
            "  __typename id handle title bodyHtml vendor productType tags "
            "  options { name values } "
            "  variants(first: 20) { edges { node { "
            "    __typename id price sku inventoryQuantity inventoryManagement inventoryPolicy weight weightUnit "
            "    selectedOptions { name value } "
            "  } } } "
            "  verwandte_produkte: metafield(namespace: \"custom\", key: \"verwandte_produkte\") { "
            "    __typename key value namespace "
            "  } "
            "  inventarbestand: metafield(namespace: \"custom\", key: \"Inventarbestand\") { "
            "    __typename key value namespace "
            "  } "
            "  price_b2b_regular: metafield(namespace: \"custom\", key: \"Price_B2B_Regular\") { "
            "    __typename key value namespace "
            "  } "
            "  price_b2b_discounted: metafield(namespace: \"custom\", key: \"Price_B2B_Discounted\") { "
            "    __typename key value namespace "
            "  } "
            "  images(first: 20) { edges { node { __typename id url } } } "
            "} } } }"
        )
        mutation = (
            "mutation bulkOperationRunQuery($query: String!) { "
            "  bulkOperationRunQuery(query: $query) { "
            "    bulkOperation { id status } "
            "    userErrors { field message } "
            "  } "
            "}"
        )
        async with httpx.AsyncClient() as client:
            resp = await self._graphql(client, mutation, {"query": bulk_query})
            return resp.json()

    async def get_current_bulk_operation(self) -> Dict[str, Any]:
        """Query the current bulk operation status."""
        query = (
            "{ currentBulkOperation { id status errorCode url createdAt completedAt objectCount fileSize } }"
        )
        async with httpx.AsyncClient() as client:
            resp = await self._graphql(client, query)
            return resp.json()

    async def fetch_bulk_result_file(self, url: str) -> List[Dict[str, Any]]:
        """Download and parse the bulk operation result (JSONL)."""
        results: List[Dict[str, Any]] = []
        async with httpx.AsyncClient() as client:
            r = await client.get(url, timeout=None)
            r.raise_for_status()
            # NDJSON: each line is a JSON object
            import json as _json
            for line in r.text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = _json.loads(line)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    results.append(obj)
        return results

    async def fetch_all_product_handles_bulk(self, poll_interval_seconds: float = 2.0, timeout_seconds: float = 300.0) -> List[Dict[str, Any]]:
        """Run a bulk export and return list of {id, handle, updatedAt}."""
        await self.start_products_bulk_handles()
        # Poll status
        import time as _time
        start = _time.time()
        url: Optional[str] = None
        while True:
            status_resp = await self.get_current_bulk_operation()
            op = (status_resp.get('data') or {}).get('currentBulkOperation') or {}
            status = op.get('status')
            if status == 'COMPLETED':
                url = op.get('url')
                break
            if status in ('FAILED', 'CANCELED'):  # pragma: no cover
                raise RuntimeError(f"Bulk operation failed: {op}")
            if _time.time() - start > timeout_seconds:
                raise TimeoutError("Bulk operation timed out")
            await asyncio.sleep(poll_interval_seconds)
        if not url:
            return []
        return await self.fetch_bulk_result_file(url)

    async def fetch_all_products_bulk_full(self, poll_interval_seconds: float = 2.0, timeout_seconds: float = 600.0) -> List[Dict[str, Any]]:
        """Run a bulk export for all products with full fields and map to REST-like dicts."""
        await self.start_products_bulk_full()
        # Poll status
        import time as _time
        start = _time.time()
        url: Optional[str] = None
        while True:
            status_resp = await self.get_current_bulk_operation()
            op = (status_resp.get('data') or {}).get('currentBulkOperation') or {}
            status = op.get('status')
            if status == 'COMPLETED':
                url = op.get('url')
                break
            if status in ('FAILED', 'CANCELED'):
                raise RuntimeError(f"Bulk operation failed: {op}")
            if _time.time() - start > timeout_seconds:
                raise TimeoutError("Bulk operation timed out")
            await asyncio.sleep(poll_interval_seconds)
        if not url:
            return []
        # Download NDJSON and map each product node
        results_raw = await self.fetch_bulk_result_file(url)

        def _gid_to_numeric_id(gid: Optional[str]) -> Optional[int]:
            if not gid or not isinstance(gid, str):
                return None
            try:
                return int(gid.rsplit('/', 1)[-1])
            except Exception:
                return None

        def _parse_metafield_value(value):
            """Parse metafield value, handling JSON strings"""
            if not value:
                return value
            
            # If it's already a list, return as-is
            if isinstance(value, list):
                return value
                
            # If it's a string that looks like JSON, try to parse it
            if isinstance(value, str):
                value = value.strip()
                if value.startswith('[') and value.endswith(']'):
                    try:
                        import json
                        return json.loads(value)
                    except json.JSONDecodeError:
                        return value
                elif value.startswith('"[') and value.endswith(']"'):
                    # Handle double-encoded JSON strings
                    try:
                        import json
                        return json.loads(json.loads(value))
                    except json.JSONDecodeError:
                        return value
            
            return value

        def process_bulk_results(results_raw):
            # Separate nodes by type
            products = {}
            variants_by_parent = {}
            images_by_parent = {}
            metafields_by_parent = {}
            
            for node in results_raw:
                if not isinstance(node, dict):
                    continue
                    
                node_type = node.get('__typename')
                
                if node_type == 'Product':
                    product_id = node.get('id')
                    if product_id:
                        products[product_id] = node
                        
                elif node_type == 'ProductVariant':
                    parent_id = node.get('__parentId')
                    if parent_id:
                        if parent_id not in variants_by_parent:
                            variants_by_parent[parent_id] = []
                        variants_by_parent[parent_id].append(node)
                        
                elif node_type == 'ProductImage':
                    parent_id = node.get('__parentId')
                    if parent_id:
                        if parent_id not in images_by_parent:
                            images_by_parent[parent_id] = []
                        images_by_parent[parent_id].append(node)

                elif node_type == 'Metafield':
                    parent_id = node.get('__parentId')
                    if parent_id:
                        if parent_id not in metafields_by_parent:
                            metafields_by_parent[parent_id] = {}
                        # Store metafields by key for easy access
                        key = node.get('key')
                        if key:
                            metafields_by_parent[parent_id][key] = node
            
            # Map products with their variants and images
            mapped = []
            seen_handles = set()
            
            for product_id, product_node in products.items():
                handle = product_node.get('handle')
                if handle and handle in seen_handles:
                    continue
                    
                # Build the mapped product
                mapped_product = {
                    "id": _gid_to_numeric_id(product_node.get("id")),
                    "handle": product_node.get("handle"),
                    "title": product_node.get("title"),
                    "body_html": product_node.get("bodyHtml"),
                    "vendor": product_node.get("vendor"),
                    "product_type": product_node.get("productType"),
                    "tags": product_node.get("tags") or [],
                }
                
                # Add options
                options = []
                for o in (product_node.get("options") or []):
                    options.append({"name": o.get("name"), "values": o.get("values") or []})
                if options:
                    mapped_product["options"] = options
                
                 # Add metafields - both from nested structure and separate nodes
                metafields = {}
        
                if 'verwandte_produkte' in product_node and product_node['verwandte_produkte']:
                    raw_value = product_node['verwandte_produkte'].get('value')
                    metafields['verwandte_produkte'] = _parse_metafield_value(raw_value)
        
                if 'inventarbestand' in product_node and product_node['inventarbestand']:
                    raw_value = product_node['inventarbestand'].get('value')
                    metafields['Inventarbestand'] = _parse_metafield_value(raw_value)

                if 'StockNextDelivery' in product_node and product_node['StockNextDelivery']:
                    raw_value = product_node['StockNextDelivery'].get('value')
                    metafields['StockNextDelivery'] = _parse_metafield_value(raw_value)

                if 'price_b2b_regular' in product_node and product_node['price_b2b_regular']:
                    raw_value = product_node['price_b2b_regular'].get('value')
                    metafields['Price_B2B_Regular'] = _parse_metafield_value(raw_value)

                if 'price_b2b_discounted' in product_node and product_node['price_b2b_discounted']:
                    raw_value = product_node['price_b2b_discounted'].get('value')
                    metafields['Price_B2B_Discounted'] = _parse_metafield_value(raw_value)

                # Also handle metafields from edges within the product node
                for edge in (product_node.get("metafields", {}).get("edges") or []):
                    metafield = edge.get("node", {})
                    key = metafield.get("key")
                    if key:
                        metafields[key] = metafield.get("value")
                
                if metafields:
                    mapped_product["metafields"] = metafields

                # Add variants from separate nodes
                variants = []
                for variant_node in variants_by_parent.get(product_id, []):
                    sel = [so.get("value") for so in (variant_node.get("selectedOptions") or [])]
                    variants.append({
                        "id": _gid_to_numeric_id(variant_node.get("id")),
                        "price": variant_node.get("price"),
                        "sku": variant_node.get("sku"),
                        "inventory_quantity": variant_node.get("inventoryQuantity"),
                        "inventory_management": variant_node.get("inventoryManagement"),
                        "inventory_policy": variant_node.get("inventoryPolicy"),
                        "weight": variant_node.get("weight"),
                        "weight_unit": variant_node.get("weightUnit"),
                        "option1": sel[0] if len(sel) > 0 else None,
                        "option2": sel[1] if len(sel) > 1 else None,
                        "option3": sel[2] if len(sel) > 2 else None,
                    })
                if variants:
                    mapped_product["variants"] = variants
                
                # Add images from separate nodes
                images = []
                for image_node in images_by_parent.get(product_id, []):
                    src = image_node.get("url")
                    if src:
                        images.append({"src": src})
                if images:
                    mapped_product["images"] = images
                
                mapped.append(mapped_product)
                if handle:
                    seen_handles.add(handle)
            
            return mapped

        # Keep only top-level Product nodes; bulk returns child nodes (variants/images) as separate lines
        product_nodes = process_bulk_results(results_raw)
        logger.info(f"Fetched {len(product_nodes)} products from Shopify via GraphQL Bulk")
        return product_nodes

   
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
 