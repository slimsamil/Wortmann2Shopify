import httpx
import asyncio
from typing import List, Dict, Any, Optional
import time as _time
from app.core.config import settings
from app.models.shopify import ShopifyProductWrapper
from app.utils.helpers import gid_to_numeric_id, parse_metafield_value
import logging
from pydantic import BaseModel
 
logger = logging.getLogger(__name__)
 
 
class ShopifyService:
    def __init__(self):
        self.shop_url = settings.shopify_shop_url
        self.access_token = settings.shopify_access_token
        self.api_version = settings.shopify_api_version
        self.headers = {
            'Content-Type': 'application/json',
            'X-Shopify-Access-Token': self.access_token
        }
        self._primary_location_id: Optional[int] = None
        self._last_rest_ts: float = 0.0

    async def _rest_call(self, client: httpx.AsyncClient, method: str, path: str, json: Optional[Dict[str, Any]] = None,
                         max_retries: int = 6) -> httpx.Response:
        """Rate-limited REST call with retry/backoff for 429/5xx.
        Enforces ~2 req/sec by spacing calls at least 0.55s apart.
        Path should be like f"/admin/api/{self.api_version}/..."
        """
        # Space out requests to ~2 rps
        since = _time.time() - self._last_rest_ts
        if since < 0.55:
            await asyncio.sleep(0.55 - since)
        url = f"{self.shop_url}{path}"
        backoff = 0.6
        for attempt in range(max_retries):
            try:
                resp = await client.request(method.upper(), url, headers=self.headers, json=json, timeout=60.0)
                self._last_rest_ts = _time.time()
                if resp.status_code in (429, 500, 502, 503, 504):
                    # Respect Retry-After if present
                    retry_after = resp.headers.get('Retry-After')
                    if retry_after:
                        try:
                            wait_s = float(retry_after)
                        except Exception:
                            wait_s = backoff
                    else:
                        wait_s = backoff
                    await asyncio.sleep(wait_s)
                    backoff = min(backoff * 2, 8)
                    continue
                return resp
            except httpx.ReadTimeout:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 8)
                continue
        return resp
   
    async def test_connection(self) -> bool:
        """Test Shopify API connection"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.shop_url}/admin/api/{self.api_version}/shop.json",
                    headers={'X-Shopify-Access-Token': self.access_token}
                )
                if response.status_code != 200:
                    logger.error(f"Shopify connection test non-200: {response.status_code} - {response.text}")
                return response.status_code == 200
        except Exception as e:
            logger.error(f"Shopify connection test failed: {str(e)}")
            return False
   
    async def fetch_all_products(self, limit: int = 250) -> List[Dict[str, Any]]:
        """Fetch all products using GraphQL pagination and return REST-like dicts.
        The "limit" parameter is retained for compatibility but GraphQL will paginate at 250/page.
        """

        def _map_product(node: Dict[str, Any]) -> Dict[str, Any]:
            # Map GraphQL product node to REST-like product dict consumed by downstream code
            mapped: Dict[str, Any] = {
                "id": gid_to_numeric_id(node.get("id")),
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
            "          price sku inventoryQuantity "
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
                    # Sanity check: GraphQL errors
                    if isinstance(data.get("errors"), list) and data["errors"]:
                        logger.error(f"GraphQL error on products fetch: {data['errors']}")
                        return all_products
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

        def _map(node: Dict[str, Any]) -> Dict[str, Any]:
            out = {
                "id": gid_to_numeric_id(node.get("id")),
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
            "      price sku inventoryQuantity "
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
                if isinstance(data.get("errors"), list) and data["errors"]:
                    logger.error(f"GraphQL error fetching product {handle}: {data['errors']}")
                    return None
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
                            f"{self.shop_url}/admin/api/{self.api_version}/products.json?handle={handle}",
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
            # Ensure inventory_management is set correctly for multiple variants (warranty options)
            variants = product_data.get('variants', []) or []
            if len(variants) > 1:
                for variant in variants:
                    if isinstance(variant, dict):
                        variant['inventory_management'] = None
                logger.info(f"Set inventory_management=None for all variants in product {shopify_id} (multiple variants detected)")
            
            async with httpx.AsyncClient() as client:
                response = await client.put(
                    f"{self.shop_url}/admin/api/{self.api_version}/products/{shopify_id}.json",
                    headers=self.headers,
                    json={"product": product_data},
                    timeout=30.0
                )
               
                if response.status_code == 200:
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
            # Resolve product id by handle or fallback strategies
            existing_product = await self.get_product_by_handle(handle)
            if not existing_product:
                product_id = await self._resolve_product_id_by_handle_or_sku(handle)
                if not product_id:
                    return {"status": "error", "message": f"Product with handle '{handle}' not found in Shopify"}
                existing_product = {"id": product_id, "handle": handle}
           
            product_id = existing_product['id']
            # Prepare the product data for Shopify API
            variants = product_data.get('variants', []) or []
            
            # Ensure inventory_management is set correctly for multiple variants (warranty options)
            if len(variants) > 1:
                for variant in variants:
                    if isinstance(variant, dict):
                        variant['inventory_management'] = None
                logger.info(f"Set inventory_management=None for all variants in product {handle} (multiple variants detected)")
            
            shopify_product_data = {
                "product": {
                    "title": product_data.get('title'),
                    "body_html": product_data.get('body_html'),
                    "vendor": product_data.get('vendor'),
                    "product_type": product_data.get('product_type'),
                    "tags": product_data.get('tags'),
                    "variants": variants,
                    "options": product_data.get('options', []),
                    "metafields": product_data.get('metafields', [])
                }
            }
           
            # Add images if present
            if product_data.get('images'):
                shopify_product_data["product"]["images"] = product_data['images']
           
            async with httpx.AsyncClient() as client:
                response = await self._rest_call(client, 'PUT', f"/admin/api/{self.api_version}/products/{product_id}.json", json=shopify_product_data)
               
                if response.status_code == 200:
                    updated_product = response.json()
                    logger.info(f"Successfully updated product {handle} in Shopify")

                    # Ensure inventory levels are updated, since Product PUT ignores inventory quantities
                    try:
                        # Skip inventory updates when multiple variants exist (warranty-only variants)
                        variants_list = product_data.get('variants', []) or []
                        if len(variants_list) <= 1:
                            await self._update_inventory_for_product(product_id, variants_list)
                        else:
                            logger.info(f"Skipping inventory update for {handle} due to multiple variants (warranty options)")
                    except Exception as inv_e:
                        logger.warning(f"Inventory update skipped/failed for {handle}: {str(inv_e)}")

                    # Ensure StockNextDelivery metafield reflects empty values by deleting when empty
                    try:
                        desired_snd = None
                        for mf in (product_data.get('metafields') or []):
                            if (mf.get('namespace') == 'custom' and mf.get('key') == 'StockNextDelivery'):
                                desired_snd = mf.get('value')
                                break
                        await self._sync_stock_next_delivery_metafield(product_id, desired_snd)
                    except Exception as mf_e:
                        logger.warning(f"StockNextDelivery metafield sync skipped/failed for {handle}: {str(mf_e)}")

                    # Ensure accessory products metafield is properly handled (deleted when empty)
                    try:
                        desired_accessories = None
                        for mf in (product_data.get('metafields') or []):
                            if (mf.get('namespace') == 'custom' and mf.get('key') == 'verwandte_produkte'):
                                desired_accessories = mf.get('value')
                                break
                        await self._sync_accessory_products_metafield(product_id, desired_accessories)
                    except Exception as mf_e:
                        logger.warning(f"Accessory products metafield sync skipped/failed for {handle}: {str(mf_e)}")

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

    async def _resolve_product_id_by_handle_or_sku(self, handle: str) -> Optional[int]:
        """Try to find a product id by handle variations or SKU parsed from handle (e.g., prod-123 -> sku:123)."""
        # 1) Try case variations of handle
        candidates = {handle}
        try:
            candidates.add(handle.lower())
            candidates.add(handle.upper())
            if handle.startswith('prod-'):
                pid = handle[5:]
                candidates.add(f"prod-{pid}")
                candidates.add(f"PROD-{pid}")
        except Exception:
            pass
        for h in candidates:
            found = await self.get_product_by_handle(h)
            if found and found.get('id'):
                return found['id']

        # 2) Try GraphQL product search by variant SKU if handle resembles prod-<id>
        pid_from_handle: Optional[str] = None
        try:
            if handle.lower().startswith('prod-'):
                pid_from_handle = handle.split('-', 1)[1]
        except Exception:
            pid_from_handle = None

        if not pid_from_handle:
            return None

        query = (
            "query($q:String!){ products(first:1, query:$q){ edges { node { id handle variants(first:5){ nodes { sku } } } } } }"
        )
        try:
            async with httpx.AsyncClient() as client:
                resp = await self._graphql(client, query, {"q": f"sku:{pid_from_handle}"})
                if resp.status_code != 200:
                    return None
                data = resp.json() or {}
                edges = (((data.get('data') or {}).get('products') or {}).get('edges') or [])
                for edge in edges:
                    node = (edge or {}).get('node') or {}
                    gid = node.get('id')
                    if gid and isinstance(gid, str):
                        try:
                            return int(gid.rsplit('/', 1)[-1])
                        except Exception:
                            continue
        except Exception:
            return None
        return None

    async def _get_primary_location_id(self, client: httpx.AsyncClient) -> Optional[int]:
        if self._primary_location_id:
            return self._primary_location_id
        resp = await self._rest_call(client, 'GET', f"/admin/api/{self.api_version}/locations.json")
        resp = resp  # keep type hints happy
        if resp.status_code != 200:
            logger.error(f"Failed to fetch locations: {resp.status_code} - {resp.text}")
            return None
        data = resp.json() or {}
        locations = data.get('locations') or []
        if not locations:
            return None
        # Prefer active and primary locations first
        primary = None
        for loc in locations:
            if loc.get('active'):
                primary = loc
                break
        if not primary:
            primary = locations[0]
        try:
            self._primary_location_id = int(primary.get('id'))
        except Exception:
            self._primary_location_id = None
        return self._primary_location_id

    async def _update_inventory_for_product(self, product_id: int, expected_variants: List[Dict[str, Any]]):
        """Update inventory levels for a single-variant product by matching on SKU.
        expected_variants should contain 'sku' and 'inventory_quantity' values from our payload.
        This assumes single variant per product - multi-variant logic handled elsewhere.
        """
        if not expected_variants:
            return
        
        async with httpx.AsyncClient() as client:
            # 1) Fetch REST product to obtain inventory_item_id for the variant
            resp = await self._rest_call(client, 'GET', f"/admin/api/{self.api_version}/products/{product_id}.json")
            if resp.status_code != 200:
                logger.warning(f"Cannot fetch product {product_id} for inventory: {resp.status_code}")
                return
            
            product = (resp.json() or {}).get('product') or {}
            variants = product.get('variants') or []
            
            if not variants:
                logger.warning(f"Product {product_id} has no variants")
                return
            
            # Get the single variant (first one since this is single-variant product)
            variant = variants[0]
            variant_id = variant.get('id')
            inventory_item_id = variant.get('inventory_item_id')
            inventory_management = variant.get('inventory_management')
            sku = variant.get('sku')
            
            if not inventory_item_id:
                logger.warning(f"Product {product_id} variant has no inventory_item_id")
                return

            # 2) Get primary location
            location_id = await self._get_primary_location_id(client)
            if not location_id:
                logger.warning("No primary location id available; skipping inventory update")
                return

            # 3) Get the expected quantity from our payload
            expected_qty = None
            for ev in expected_variants:
                payload_sku = ev.get('sku')
                qty = ev.get('inventory_quantity')
                # Match by SKU if available, otherwise use first entry
                if not sku or str(payload_sku) == str(sku) or not payload_sku:
                    expected_qty = qty
                    break
            
            if expected_qty is None:
                logger.warning(f"No inventory_quantity found in payload for product {product_id}")
                return

            # 4) Ensure inventory tracking is enabled
            tracking_enabled = inventory_management and str(inventory_management).lower() != 'none'
            
            if not tracking_enabled:
                # Enable inventory management on variant
                try:
                    put_body = {"variant": {"id": variant_id, "inventory_management": "shopify"}}
                    v_put = await self._rest_call(
                        client, 
                        'PUT', 
                        f"/admin/api/{self.api_version}/products/{product_id}/variants/{variant_id}.json", 
                        json=put_body
                    )
                    if v_put.status_code not in (200, 201):
                        logger.warning(f"Failed enabling inventory_management for variant {variant_id}: {v_put.status_code} - {v_put.text}")
                        return
                except Exception as e:
                    logger.warning(f"Error enabling inventory_management for variant {variant_id}: {str(e)}")
                    return
                
                # Enable tracking on inventory item
                try:
                    ii_body = {"inventory_item": {"id": int(inventory_item_id), "tracked": True}}
                    ii_put = await self._rest_call(
                        client, 
                        'PUT', 
                        f"/admin/api/{self.api_version}/inventory_items/{int(inventory_item_id)}.json", 
                        json=ii_body
                    )
                    if ii_put.status_code not in (200, 201):
                        logger.warning(f"Failed enabling tracked for inventory_item {inventory_item_id}: {ii_put.status_code} - {ii_put.text}")
                        return
                except Exception as e:
                    logger.warning(f"Error enabling tracked for inventory_item {inventory_item_id}: {str(e)}")
                    return

            # 5) Set inventory level
            set_body = {
                "location_id": location_id,
                "inventory_item_id": inventory_item_id,
                "available": int(expected_qty)
            }
            
            try:
                set_resp = await self._rest_call(
                    client, 
                    'POST', 
                    f"/admin/api/{self.api_version}/inventory_levels/set.json", 
                    json=set_body
                )
                
                if set_resp.status_code not in (200, 201):
                    logger.warning(
                        f"Inventory set failed for product {product_id}, variant {variant_id}, "
                        f"inventory_item {inventory_item_id}: {set_resp.status_code} - {set_resp.text}"
                    )
                else:
                    logger.info(f"Successfully updated inventory for product {product_id} to {expected_qty}")
                    
            except Exception as e:
                logger.warning(f"Exception setting inventory for product {product_id}: {str(e)}")

    async def _sync_stock_next_delivery_metafield(self, product_id: int, desired_value: Optional[str]) -> None:
        """Ensure the product's StockNextDelivery metafield matches desired_value.
        If desired_value is falsy (None/""), delete the metafield if it exists.
        If desired_value is non-empty, upsert it.
        """
        async with httpx.AsyncClient() as client:
            # 1) List existing product metafields
            list_resp = await self._rest_call(client, 'GET', f"/admin/api/{self.api_version}/products/{product_id}/metafields.json")
            if list_resp.status_code != 200:
                logger.warning(f"Cannot list metafields for product {product_id}: {list_resp.status_code}")
                return
            metafields = (list_resp.json() or {}).get('metafields') or []
            target = None
            for mf in metafields:
                if mf.get('namespace') == 'custom' and mf.get('key') == 'StockNextDelivery':
                    target = mf
                    break

            # 2) If desired empty -> delete if exists
            if not desired_value:
                if target and target.get('id'):
                    del_resp = await self._rest_call(client, 'DELETE', f"/admin/api/{self.api_version}/metafields/{target['id']}.json")
                    if del_resp.status_code not in (200, 204):
                        logger.warning(f"Failed deleting StockNextDelivery for product {product_id}: {del_resp.status_code} - {del_resp.text}")
                return

            # 3) desired non-empty -> upsert
            if target and target.get('id'):
                put_body = {
                    "metafield": {
                        "id": target['id'],
                        "value": desired_value,
                        "type": "single_line_text_field"
                    }
                }
                put_resp = await self._rest_call(client, 'PUT', f"/admin/api/{self.api_version}/metafields/{target['id']}.json", json=put_body)
                if put_resp.status_code != 200:
                    logger.warning(f"Failed updating StockNextDelivery for product {product_id}: {put_resp.status_code} - {put_resp.text}")
                return

            # Create new metafield
            post_body = {
                "metafield": {
                    "namespace": "custom",
                    "key": "StockNextDelivery",
                    "owner_id": product_id,
                    "owner_resource": "product",
                    "type": "single_line_text_field",
                    "value": desired_value
                }
            }
            post_resp = await self._rest_call(client, 'POST', f"/admin/api/{self.api_version}/metafields.json", json=post_body)
            if post_resp.status_code not in (200, 201):
                logger.warning(f"Failed creating StockNextDelivery for product {product_id}: {post_resp.status_code} - {post_resp.text}")
   
    async def _sync_accessory_products_metafield(self, product_id: int, desired_value: Optional[str]) -> None:
        """Ensure the product's verwandte_produkte metafield matches desired_value.
        If desired_value is falsy (None/""), delete the metafield if it exists.
        If desired_value is non-empty, upsert it.
        """
        async with httpx.AsyncClient() as client:
            # 1) List existing product metafields
            list_resp = await self._rest_call(client, 'GET', f"/admin/api/{self.api_version}/products/{product_id}/metafields.json")
            if list_resp.status_code != 200:
                logger.warning(f"Cannot list metafields for product {product_id}: {list_resp.status_code}")
                return
            metafields = (list_resp.json() or {}).get('metafields') or []
            target = None
            for mf in metafields:
                if mf.get('namespace') == 'custom' and mf.get('key') == 'verwandte_produkte':
                    target = mf
                    break

            # 2) If desired empty -> delete if exists
            if not desired_value:
                if target and target.get('id'):
                    del_resp = await self._rest_call(client, 'DELETE', f"/admin/api/{self.api_version}/metafields/{target['id']}.json")
                    if del_resp.status_code not in (200, 204):
                        logger.warning(f"Failed deleting verwandte_produkte for product {product_id}: {del_resp.status_code} - {del_resp.text}")
                return

            # 3) desired non-empty -> upsert
            if target and target.get('id'):
                put_body = {
                    "metafield": {
                        "id": target['id'],
                        "value": desired_value,
                        "type": "json"
                    }
                }
                put_resp = await self._rest_call(client, 'PUT', f"/admin/api/{self.api_version}/metafields/{target['id']}.json", json=put_body)
                if put_resp.status_code != 200:
                    logger.warning(f"Failed updating verwandte_produkte for product {product_id}: {put_resp.status_code} - {put_resp.text}")
                return

            # Create new metafield
            post_body = {
                "metafield": {
                    "namespace": "custom",
                    "key": "verwandte_produkte",
                    "owner_id": product_id,
                    "owner_resource": "product",
                    "type": "json",
                    "value": desired_value
                }
            }
            post_resp = await self._rest_call(client, 'POST', f"/admin/api/{self.api_version}/metafields.json", json=post_body)
            if post_resp.status_code not in (200, 201):
                logger.warning(f"Failed creating verwandte_produkte for product {product_id}: {post_resp.status_code} - {post_resp.text}")

    async def delete_product_by_id(self, product_id: int) -> Dict[str, Any]:
        """Delete a Shopify product by its numeric Shopify ID."""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.delete(
                    f"{self.shop_url}/admin/api/{self.api_version}/products/{product_id}.json",
                    headers=self.headers,
                    timeout=30.0
                )
                if response.status_code in (200, 204):
                    return {"status": "success", "shopify_id": product_id}
                return {"status": "error", "shopify_id": product_id, "response": response.text, "code": response.status_code}
        except Exception as e:
            logger.error(f"Error deleting Shopify product {product_id}: {str(e)}")
            return {"status": "error", "shopify_id": product_id, "error": str(e)}
   
    async def send_products_batch(self, products: List[ShopifyProductWrapper], batch_size: int = 10) -> List[Dict[str, Any]]:
        """Send products to Shopify in robustly limited batches for large sets."""
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
            f"{self.shop_url}/admin/api/{self.api_version}/graphql.json",
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
            data = resp.json() or {}
            # Sanity: log userErrors if present
            ue = (((data.get('data') or {}).get('bulkOperationRunQuery') or {}).get('userErrors') or [])
            if ue:
                logger.error(f"Bulk handles start userErrors: {ue}")
            return data

    async def start_products_bulk_full(self) -> Dict[str, Any]:
        """Start a GraphQL Bulk Operation to export all products with full known fields."""
        bulk_query = (
            "{ products { edges { node { "
            "  __typename id handle title bodyHtml vendor productType tags "
            "  options { name values } "
            "  variants(first: 20) { edges { node { "
            "    __typename id price sku inventoryQuantity "
            "    selectedOptions { name value } "
            "  } } } "
            "  verwandte_produkte: metafield(namespace: \"custom\", key: \"verwandte_produkte\") { "
            "    __typename key value namespace "
            "  } "
            "  inventarbestand: metafield(namespace: \"custom\", key: \"Inventarbestand\") { "
            "    __typename key value namespace "
            "  } "
            "  stock_next_delivery: metafield(namespace: \"custom\", key: \"StockNextDelivery\") { "
            "    __typename key value namespace "
            "  } "
            "  warranty_group: metafield(namespace: \"custom\", key: \"warranty_group\") { "
            "    __typename key value namespace "
            "  } "
            "  price_b2b_regular: metafield(namespace: \"custom\", key: \"Price_B2B_Regular\") { "
            "    __typename key value namespace "
            "  } "
            "  price_b2b_discounted: metafield(namespace: \"custom\", key: \"Price_B2B_Discounted\") { "
            "    __typename key value namespace "
            "  } "
            "  prozessorfamilie: metafield(namespace: \"custom\", key: \"Prozessorfamilie\") { "
            "    __typename key value namespace "
            "  } "
            "  speicher: metafield(namespace: \"custom\", key: \"Speicher\") { "
            "    __typename key value namespace "
            "  } "
            "  ram: metafield(namespace: \"custom\", key: \"RAM\") { "
            "    __typename key value namespace "
            "  } "
            "  gpu: metafield(namespace: \"custom\", key: \"GPU\") { "
            "    __typename key value namespace "
            "  } "
            "  prozessor: metafield(namespace: \"custom\", key: \"Prozessor\") { "
            "    __typename key value namespace "
            "  } "
            "  bildschirmdiagonale: metafield(namespace: \"custom\", key: \"Bildschirmdiagonale\") { "
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
            data = resp.json() or {}
            ue = (((data.get('data') or {}).get('bulkOperationRunQuery') or {}).get('userErrors') or [])
            if ue:
                logger.error(f"Bulk full start userErrors: {ue}")
            return data

    async def get_current_bulk_operation(self) -> Dict[str, Any]:
        """Query the current bulk operation status."""
        query = (
            "{ currentBulkOperation { id status errorCode url createdAt completedAt objectCount fileSize } }"
        )
        async with httpx.AsyncClient() as client:
            resp = await self._graphql(client, query)
            data = resp.json() or {}
            op = (data.get('data') or {}).get('currentBulkOperation') or {}
            status = op.get('status')
            error_code = op.get('errorCode')
            if status and status not in ('COMPLETED', 'RUNNING', 'CREATED'):  # surface unexpected
                logger.warning(f"Bulk op status: {status} code: {error_code}")
            return data

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
                    "id": gid_to_numeric_id(product_node.get("id")),
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
                    metafields['verwandte_produkte'] = parse_metafield_value(raw_value)
        
                if 'inventarbestand' in product_node and product_node['inventarbestand']:
                    raw_value = product_node['inventarbestand'].get('value')
                    metafields['Inventarbestand'] = parse_metafield_value(raw_value)

                if 'stock_next_delivery' in product_node and product_node['stock_next_delivery']:
                    raw_value = product_node['stock_next_delivery'].get('value')
                    metafields['StockNextDelivery'] = parse_metafield_value(raw_value)

                if 'warranty_group' in product_node and product_node['warranty_group']:
                    raw_value = product_node['warranty_group'].get('value')
                    metafields['warranty_group'] = parse_metafield_value(raw_value)    

                if 'price_b2b_regular' in product_node and product_node['price_b2b_regular']:
                    raw_value = product_node['price_b2b_regular'].get('value')
                    metafields['Price_B2B_Regular'] = parse_metafield_value(raw_value)

                if 'price_b2b_discounted' in product_node and product_node['price_b2b_discounted']:
                    raw_value = product_node['price_b2b_discounted'].get('value')
                    metafields['Price_B2B_Discounted'] = parse_metafield_value(raw_value)

                if 'ram' in product_node and product_node['ram']:
                    raw_value = product_node['ram'].get('value')
                    metafields['RAM'] = parse_metafield_value(raw_value)

                if 'prozessor' in product_node and product_node['prozessor']:
                    raw_value = product_node['prozessor'].get('value')
                    metafields['Prozessor'] = parse_metafield_value(raw_value)

                if 'prozessorfamilie' in product_node and product_node['prozessorfamilie']:
                    raw_value = product_node['prozessorfamilie'].get('value')
                    metafields['Prozessorfamilie'] = parse_metafield_value(raw_value)

                if 'gpu' in product_node and product_node['gpu']:
                    raw_value = product_node['gpu'].get('value')
                    metafields['GPU'] = parse_metafield_value(raw_value)

                if 'speicher' in product_node and product_node['speicher']:
                    raw_value = product_node['speicher'].get('value')
                    metafields['Speicher'] = parse_metafield_value(raw_value)

                if 'bildschirmdiagonale' in product_node and product_node['bildschirmdiagonale']:
                    raw_value = product_node['bildschirmdiagonale'].get('value')
                    metafields['Bildschirmdiagonale'] = parse_metafield_value(raw_value)

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
                        "id": gid_to_numeric_id(variant_node.get("id")),
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
        try:
            response = await client.post(
                f"{self.shop_url}/admin/api/{self.api_version}/products.json",
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
                    'status_code': response.status_code,
                }
        except Exception as e:
            return {
                'status': 'error',
                'product_id': product.product.handle,
                'error': str(e)
            }
 
 
shopify_service = ShopifyService()
 