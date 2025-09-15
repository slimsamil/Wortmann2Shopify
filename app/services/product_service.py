from collections import defaultdict
from decimal import Decimal
import json
import logging
from typing import Dict, List

from app.models.shopify import (
    ShopifyImage,
    ShopifyMetafield,
    ShopifyOption,
    ShopifyProduct,
    ShopifyProductWrapper,
    ShopifyVariant,
)
from app.utils.helpers import to_base64

logger = logging.getLogger(__name__)


class ProductService:
    def merge_data(self, products: List[Dict], images: List[Dict], warranties: List[Dict]) -> List[Dict]:
        """Merge products with images and warranties"""
        try:
            # Create lookup dictionaries
            images_by_product = defaultdict(list)
            for image in images:
                supplier_aid = image.get('supplier_aid')
                if supplier_aid:
                    images_by_product[supplier_aid].append(image)

            # Sort images per product so that primary images come first
            def _to_int(value):
                try:
                    return int(value)
                except Exception:
                    return 0

            for _supplier_aid, _image_list in images_by_product.items():
                # Place records with IsPrimary == 1 before others; keep original order among equals (Python sort is stable)
                _image_list.sort(key=lambda img: _to_int(img.get('IsPrimary')), reverse=True)
            
            warranties_by_group = defaultdict(list)
            for warranty in warranties:
                group = warranty.get('garantiegruppe')
                if group is not None:
                    warranties_by_group[group].append(warranty)
            
            # Merge data
            merged_items = []
            for product in products:
                product_id = product.get('ProductId')
                guarantee_group = product.get('Garantiegruppe')
                
                base_item = product.copy()
                
                # Add images
                if product_id in images_by_product:
                    for image in images_by_product[product_id]:
                        merged_item = {**base_item, **image}
                        merged_items.append(merged_item)
                
                # Add warranties
                if guarantee_group is not None and guarantee_group in warranties_by_group:
                    for warranty in warranties_by_group[guarantee_group]:
                        merged_item = {**base_item, **warranty}
                        merged_items.append(merged_item)
                
                # If no images or warranties, add base item
                if (product_id not in images_by_product and 
                    (guarantee_group is None or guarantee_group not in warranties_by_group)):
                    merged_items.append(base_item)
            
            logger.info(f"Merged data resulted in {len(merged_items)} items")
            return merged_items
        except Exception as e:
            logger.error(f"Error merging data: {str(e)}")
            raise
    
    def process_products(self, items: List[Dict]) -> List[ShopifyProductWrapper]:
        """Process merged items into Shopify product format"""
        try:
            product_map = {}
            
            # Group items by ProductId
            for item in items:
                try:
                    product_id = item.get('ProductId')
                    if not product_id:
                        logger.warning(f"Skipping item without ProductId: {item}")
                        continue
                    
                    if product_id not in product_map:
                        product_map[product_id] = {
                            **item,
                            '_images': [],
                            '_images_seen': set(),
                            '_warranties': []
                        }
                    
                    current = product_map[product_id]
                    
                    # Process images
                    img_raw = (item.get('base64') or 
                              item.get('hex') or 
                              (item.get('images', [{}])[0].get('base64') if isinstance(item.get('images'), list) else None))
                    
                    if img_raw:
                        try:
                            img_val = to_base64(img_raw)
                            if img_val and img_val not in current['_images_seen']:
                                current['_images'].append(img_val)
                                current['_images_seen'].add(img_val)
                        except Exception as e:
                            logger.warning(f"Error processing image for product {product_id}: {str(e)}")
                    
                    # Process warranties
                    if item.get('name') and item.get('prozentsatz') is not None:
                        current['_warranties'].append({
                            'id': item.get('id'),
                            'name': item.get('name'),
                            'monate': item.get('monate'),
                            'prozentsatz': item.get('prozentsatz'),
                            'minimum': item.get('minimum'),
                            'garantiegruppe': item.get('garantiegruppe')
                        })
                except Exception as e:
                    logger.warning(f"Error processing item for product {item.get('ProductId', 'unknown')}: {str(e)}")
                    continue
            
            # Convert to Shopify format
            shopify_products = []
            for product_id, product in product_map.items():
                try:
                    shopify_product = self._create_shopify_product(product)
                    shopify_products.append(shopify_product)
                except Exception as e:
                    logger.error(f"Error creating Shopify product for {product_id}: {str(e)}")
                    # Continue with other products instead of failing completely
                    continue
            
            logger.info(f"Processed {len(shopify_products)} products for Shopify")
            return shopify_products
        except Exception as e:
            logger.error(f"Error processing products: {str(e)}")
            raise
    
    def _create_shopify_product(self, product: Dict) -> ShopifyProductWrapper:
        """Create Shopify product structure"""
        product_id = product.get('ProductId', 'unknown')
        logger.debug(f"Creating Shopify product for {product_id}")
        
        try:
            base_price = float(product.get('Price_B2C_inclVAT') or product.get('Price_B2B_Regular') or 0)
            qty = int(product.get('Stock') or 0)
            weight = float(product.get('GrossWeight') or product.get('NetWeight') or 0)
            handle = f"prod-{product_id}"
            has_group = int(product.get('Garantiegruppe') or 0) != 0
        except (ValueError, TypeError) as e:
            logger.warning(f"Error parsing numeric values for product {product_id}: {str(e)}")
            # Use safe defaults
            base_price = 0.0
            qty = 0
            weight = 0.0
            has_group = False
        
        # Process images
        images = []
        for img_b64 in product.get('_images', []):
            if img_b64:
                images.append(ShopifyImage(attachment=img_b64))
        
        variants = []
        option_values = []
        
        if not has_group:
            # Simple product without warranty groups
            label = product.get('Warranty') or 'Standard'
            variants = [ShopifyVariant(
                price=f"{base_price:.2f}",
                sku=str(product.get('ProductId')),
                inventory_quantity=qty,
                inventory_management='shopify',
                inventory_policy='deny',
                weight=weight,
                weight_unit='kg',
                option1=label
            )]
            option_values = [label]
        else:
            # Product with warranty groups
            grp_value = product.get('Garantiegruppe')
            warranties = product.get('_warranties', [])
            
            # Filter warranties by group and remove duplicates
            seen_warranty_ids = set()
            filtered_warranties = []
            
            for warranty in warranties:
                if (str(warranty.get('garantiegruppe')) == str(grp_value) and 
                    warranty.get('id') not in seen_warranty_ids):
                    seen_warranty_ids.add(warranty.get('id'))
                    filtered_warranties.append(warranty)
            
            # Create variants for each warranty
            for warranty in filtered_warranties:
                try:
                    prozentsatz = warranty.get('prozentsatz', 0)
                    minimum = warranty.get('minimum', 0)
                    add_on = Decimal(str(base_price)) * prozentsatz / Decimal('100')
                    price = base_price + float(add_on)
                    sku_ext = f"G{warranty.get('id')}"
                    warranty_name = warranty.get('name', '')
                    months = warranty.get('monate', '')
                    
                    option_label = f"{warranty_name} {months} Monate"
                    option_values.append(warranty_name)
                    
                    variants.append(ShopifyVariant(
                        price=f"{price:.2f}",
                        sku=f"{product.get('ProductId')}-{sku_ext}",
                        inventory_management=None,
                        inventory_policy='deny',
                        inventory_quantity=0,
                        weight=weight,
                        weight_unit='kg',
                        option1=option_label
                    ))
                except Exception as e:
                    logger.warning(f"Error processing warranty for product {product.get('ProductId')}: {str(e)}")
                    continue
            
            # Fallback if no variants were created
            if not variants:
                label = product.get('Warranty') or 'Standard'
                variants = [ShopifyVariant(
                    price=f"{base_price:.2f}",
                    sku=str(product.get('ProductId')),
                    inventory_quantity=qty,
                    inventory_management='shopify',
                    inventory_policy='deny',
                    weight=weight,
                    weight_unit='kg',
                    option1=label
                )]
                option_values = [label]
        
        # Build metafields
        metafields = []
        if not has_group:
            metafields.append(ShopifyMetafield(
                namespace='custom',
                key='warranty',
                value=product.get('Warranty') or '',
                type='single_line_text_field'
            ))
        
        metafields.append(ShopifyMetafield(
            namespace='custom',
            key='Inventarbestand',
            value=str(qty),
            type='number_integer'
        ))

        metafields.append(ShopifyMetafield(
            namespace='custom',
            key='StockNextDelivery',
            value=product.get('StockNextDelivery') or '',
            type='single_line_text_field'
        ))

        metafields.append(ShopifyMetafield(
            namespace='custom',
            key='Price_B2B_Regular',
            value=str(product.get('Price_B2B_Regular')) or 0,
            type='number_decimal'
        ))

        metafields.append(ShopifyMetafield(
            namespace='custom',
            key='Price_B2B_Discounted',
            value=str(product.get('Price_B2B_Discounted')) or 0,
            type='number_decimal'
        ))

        if product.get('AccessoryProducts'):
            accessory_products = product.get('AccessoryProducts')
            split = accessory_products.split('|')
            prefixed_ids = []

            for id in split:
                prefixed_ids.append(f"prod-{id}")
            json_accessory_products = json.dumps(prefixed_ids)
            metafields.append(ShopifyMetafield(
                            namespace='custom',
                            key='verwandte_produkte',
                            value=json_accessory_products,
                            type='json'
                        ))
        else:
            metafields.append(ShopifyMetafield(
                            namespace='custom',
                            key='verwandte_produkte',
                            value='',
                            type='json'
                        ))
                        
        # Build final product structure
        shopify_product = ShopifyProduct(
            title=product.get('Title') or 'Untitled Product',
            handle=handle,
            body_html=product.get('LongDescription') or product.get('DescriptionShort') or '',
            vendor=product.get('Manufacturer'),
            product_type=product.get('Category'),
            variants=variants,
            options=[ShopifyOption(
                name='Garantie',
                values=list(set(option_values))
            )],
            metafields=metafields,
            images=images if images else None
        )
        
        # Add tags if CategoryPath exists
        if product.get('CategoryPath'):
            shopify_product.tags = product.get('CategoryPath').split('|')
        
        return ShopifyProductWrapper(product=shopify_product)


product_service = ProductService()
