from typing import List, Dict, Any
from app.core.database import db_manager
import logging

logger = logging.getLogger(__name__)


class DatabaseService:
    def __init__(self):
        self.db_manager = db_manager
    
    def fetch_products(self, limit: int = None) -> List[Dict[str, Any]]:
        """Fetch products from WortmannProdukte_backup table"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                if limit is not None:
                    cursor.execute(f"SELECT TOP({limit}) * FROM WortmannProdukte_backup")
                else:
                    cursor.execute("SELECT * FROM WortmannProdukte_backup")
                
                columns = [column[0] for column in cursor.description]
                products = []
                
                for row in cursor.fetchall():
                    product_dict = {}
                    for i, value in enumerate(row):
                        product_dict[columns[i]] = value
                    products.append(product_dict)
                
                logger.info(f"Fetched {len(products)} products from database")
                return products
        except Exception as e:
            logger.error(f"Error fetching products: {str(e)}")
            raise
    
    
    def fetch_products_by_ids(self, product_ids: List[str]) -> List[Dict[str, Any]]:
        """Fetch multiple products by ProductId list in a single query from WortmannProdukte_backup"""
        if not product_ids:
            return []
        
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Create placeholders for IN clause
                placeholders = ','.join(['?' for _ in product_ids])
                query = f"SELECT * FROM WortmannProdukte_backup WHERE ProductId IN ({placeholders})"
                
                cursor.execute(query, product_ids)
                
                columns = [column[0] for column in cursor.description]
                products = []
                
                for row in cursor.fetchall():
                    product_dict = {}
                    for i, value in enumerate(row):
                        product_dict[columns[i]] = value
                    products.append(product_dict)
                
                logger.info(f"Fetched {len(products)} products from database by IDs (requested: {len(product_ids)})")
                return products
                
        except Exception as e:
            logger.error(f"Error fetching products by IDs: {str(e)}")
            raise
    
    
    def fetch_images(self, limit: int = None) -> List[Dict[str, Any]]:
        """Fetch images from BilderShopify table"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                if limit is not None:
                    cursor.execute(f"SELECT TOP({limit}) * FROM BilderShopify")
                else:
                    cursor.execute("SELECT * FROM BilderShopify")
                
                columns = [column[0] for column in cursor.description]
                images = []
                
                for row in cursor.fetchall():
                    image_dict = {}
                    for i, value in enumerate(row):
                        image_dict[columns[i]] = value
                    images.append(image_dict)
                
                logger.info(f"Fetched {len(images)} images from database")
                return images
        except Exception as e:
            logger.error(f"Error fetching images: {str(e)}")
            raise
    
    def fetch_images_by_supplier_aids(self, supplier_aids: List[str]) -> List[Dict[str, Any]]:
        """Fetch images for specified supplier_aid (ProductId) values only."""
        if not supplier_aids:
            return []
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                placeholders = ','.join(['?' for _ in supplier_aids])
                query = f"SELECT * FROM BilderShopify WHERE supplier_aid IN ({placeholders})"
                cursor.execute(query, supplier_aids)
                columns = [column[0] for column in cursor.description]
                images = []
                for row in cursor.fetchall():
                    image_dict = {}
                    for i, value in enumerate(row):
                        image_dict[columns[i]] = value
                    images.append(image_dict)
                logger.info(f"Fetched {len(images)} images by supplier_aids (requested: {len(supplier_aids)})")
                return images
        except Exception as e:
            logger.error(f"Error fetching images by supplier_aids: {str(e)}")
            raise


    def fetch_warranties(self) -> List[Dict[str, Any]]:
        """Fetch warranties from GarantieOptionen table"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM GarantieOptionen")
                
                columns = [column[0] for column in cursor.description]
                warranties = []
                
                for row in cursor.fetchall():
                    warranty_dict = {}
                    for i, value in enumerate(row):
                        warranty_dict[columns[i]] = value
                    warranties.append(warranty_dict)
                
                logger.info(f"Fetched {len(warranties)} warranties from database")
                return warranties
        except Exception as e:
            logger.error(f"Error fetching warranties: {str(e)}")
            raise
    
    def fetch_warranties_by_groups(self, groups: List[int]) -> List[Dict[str, Any]]:
        """Fetch warranties limited to the specified garantiegruppe values."""
        if not groups:
            return []
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                placeholders = ','.join(['?' for _ in groups])
                query = f"SELECT * FROM GarantieOptionen WHERE garantiegruppe IN ({placeholders})"
                cursor.execute(query, groups)
                columns = [column[0] for column in cursor.description]
                warranties = []
                for row in cursor.fetchall():
                    warranty_dict = {}
                    for i, value in enumerate(row):
                        warranty_dict[columns[i]] = value
                    warranties.append(warranty_dict)
                logger.info(f"Fetched {len(warranties)} warranties by groups (requested groups: {len(groups)})")
                return warranties
        except Exception as e:
            logger.error(f"Error fetching warranties by groups: {str(e)}")
            raise
    

    def upsert_wortmann_products(self, products: List[Dict[str, Any]]) -> int:
        """Upsert Wortmann product rows into WortmannProdukte_backup table with automatic rental product enrichment."""
        if not products:
            return 0
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                affected = 0
                enriched_count = 0
                
                # Step 1: Pre-process products to enrich rental products before DB insert
                enriched_products = self._preprocess_products_for_enrichment(products)
                
                # Step 2: Insert/Update all products (including pre-enriched rental products)
                for p in enriched_products:
                    # Delete existing row for ProductId to avoid duplicates
                    cursor.execute("DELETE FROM WortmannProdukte_backup WHERE ProductId = ?", (p.get('ProductId'),))
                    cursor.execute(
                        """
                        INSERT INTO WortmannProdukte_backup (
                            ProductId, Title, DescriptionShort, LongDescription,
                            Manufacturer, Category, CategoryPath, Warranty,
                            Price_B2B_Regular, Price_B2B_Discounted, Price_B2C_inclVAT,
                            Currency, VATRate, Stock, StockNextDelivery,
                            ImagePrimary, ImageAdditional, GrossWeight, NetWeight,
                            NonReturnable, EOL, Promotion, Garantiegruppe, AccessoryProducts
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            p.get('ProductId'), p.get('Title'), p.get('DescriptionShort'), p.get('LongDescription'),
                            p.get('Manufacturer'), p.get('Category'), p.get('CategoryPath'), p.get('Warranty'),
                            p.get('Price_B2B_Regular'), p.get('Price_B2B_Discounted'), p.get('Price_B2C_inclVAT'),
                            p.get('Currency'), p.get('VATRate'), p.get('Stock'), p.get('StockNextDelivery'),
                            p.get('ImagePrimary'), p.get('ImageAdditional'), p.get('GrossWeight'), p.get('NetWeight'),
                            1 if p.get('NonReturnable') else 0, 1 if p.get('EOL') else 0, 1 if p.get('Promotion') else 0,
                            p.get('Garantiegruppe'), p.get('AccessoryProducts')
                        )
                    )
                    affected += 1
                    if p.get('_enriched'):
                        enriched_count += 1
                
                conn.commit()
                logger.info(f"Upserted {affected} Wortmann products ({enriched_count} rental products pre-enriched)")
                return affected
        except Exception as e:
            logger.error(f"Error upserting Wortmann products: {str(e)}")
            raise
    

    def insert_images_records(self, records: List[Dict[str, Any]]) -> int:
        """Insert image binaries into BilderShopify. Records require supplier_aid, filename, data (bytes), IsPrimary (0/1)."""
        if not records:
            return 0
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                affected = 0
                for r in records:
                    cursor.execute("DELETE FROM BilderShopify WHERE filename = ?", (r.get('filename'),))
                    cursor.execute(
                        """
                        INSERT INTO BilderShopify (supplier_aid, filename, base64, IsPrimary)
                        VALUES (?,?,?,?)
                        """,
                        (
                            r.get('supplier_aid'), r.get('filename'), r.get('data'), r.get('IsPrimary', 0)
                        )
                    )
                    affected += 1
                conn.commit()
                logger.info(f"Inserted {affected} image records into BilderShopify")
                return affected
        except Exception as e:
            logger.error(f"Error inserting image records: {str(e)}")
            raise
    
    
    def _preprocess_products_for_enrichment(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Pre-process products to enrich rental products (C12/C24/C36) with main product data
        before database insertion. This is much more efficient than post-processing.
        """
        try:
            # Create lookup maps for main products
            main_products_by_id = {}
            rental_products = []
            
            # First pass: separate main products and rental products
            for product in products:
                product_id = product.get('ProductId', '')
                if product_id.endswith('C12') or product_id.endswith('C24') or product_id.endswith('C36'):
                    rental_products.append(product)
                else:
                    main_products_by_id[product_id] = product
            
            logger.info(f"Found {len(main_products_by_id)} main products and {len(rental_products)} rental products")
            
            # Second pass: enrich rental products with main product data
            enriched_products = []
            enriched_count = 0
            
            for product in products:
                product_id = product.get('ProductId', '')
                
                # Check if this is a rental product that needs enrichment
                if (product_id.endswith('C12') or product_id.endswith('C24') or product_id.endswith('C36')):
                    main_product_id = self._extract_main_product_id(product_id)
                    main_product = main_products_by_id.get(main_product_id)
                    
                    if main_product:
                        # Create enriched copy of the rental product
                        enriched_product = product.copy()
                        enriched_product['LongDescription'] = main_product.get('LongDescription', '')
                        enriched_product['ImagePrimary'] = main_product.get('ImagePrimary', '')
                        enriched_product['ImageAdditional'] = main_product.get('ImageAdditional', '')
                        enriched_product['AccessoryProducts'] = main_product.get('AccessoryProducts', '')
                        enriched_product['_enriched'] = True
                        
                        enriched_products.append(enriched_product)
                        enriched_count += 1
                        
                        logger.info(f"Pre-enriched rental product {product_id} with data from main product {main_product_id}")
                    else:
                        # Keep original if no main product found
                        enriched_products.append(product)
                        logger.warning(f"Main product {main_product_id} not found for rental product {product_id}")
                else:
                    # Keep main products as-is
                    enriched_products.append(product)
            
            logger.info(f"Pre-processed {enriched_count} rental products for enrichment")
            return enriched_products
            
        except Exception as e:
            logger.error(f"Error preprocessing products for enrichment: {str(e)}")
            raise


    def _extract_main_product_id(self, rental_product_id: str) -> str:
        """
        Extract main product ID from rental product ID.
        Examples:
        - 1000035C12 -> 1000035
        - 1000035C24 -> 1000035  
        - 1000035C36 -> 1000035
        """
        # Remove C12, C24, C36 suffixes
        if rental_product_id.endswith('C12'):
            return rental_product_id[:-3]
        elif rental_product_id.endswith('C24'):
            return rental_product_id[:-3]
        elif rental_product_id.endswith('C36'):
            return rental_product_id[:-3]
        else:
            return rental_product_id

    def enrich_rental_products_with_main_product_data(self) -> int:
        """
        Enrich C12/C24/C36 rental products with data from their main product.
        Updates WortmannProdukte_backup table with missing fields from main products.
        This is a standalone function for manual enrichment of backup table.
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                affected = 0
                
                # Find all rental products (C12, C24, C36) that need enrichment
                cursor.execute("""
                    SELECT ProductId, Title, Category 
                    FROM WortmannProdukte_backup 
                    WHERE (ProductId LIKE '%C12' OR ProductId LIKE '%C24' OR ProductId LIKE '%C36')
                    AND (LongDescription IS NULL OR LongDescription = '' 
                         OR ImagePrimary IS NULL OR ImagePrimary = ''
                         OR AccessoryProducts IS NULL OR AccessoryProducts = '')
                """)
                
                rental_products = cursor.fetchall()
                logger.info(f"Found {len(rental_products)} rental products that need enrichment")
                
                for rental_product in rental_products:
                    rental_id = rental_product[0]
                    rental_title = rental_product[1]
                    rental_category = rental_product[2]
                    
                    # Extract main product ID using the helper function
                    main_product_id = self._extract_main_product_id(rental_id)
                    
                    logger.info(f"Processing rental product {rental_id}, looking for main product {main_product_id}")
                    
                    # Find the main product
                    cursor.execute("""
                        SELECT LongDescription, ImagePrimary, ImageAdditional, AccessoryProducts
                        FROM WortmannProdukte_backup 
                        WHERE ProductId = ?
                    """, (main_product_id,))
                    
                    main_product = cursor.fetchone()
                    
                    if main_product:
                        long_description, image_primary, image_additional, accessory_products = main_product
                        
                        # Update the rental product with main product data
                        cursor.execute("""
                            UPDATE WortmannProdukte_backup 
                            SET LongDescription = ?, 
                                ImagePrimary = ?, 
                                ImageAdditional = ?, 
                                AccessoryProducts = ?
                            WHERE ProductId = ?
                        """, (long_description, image_primary, image_additional, accessory_products, rental_id))
                        
                        affected += 1
                        logger.info(f"Enriched rental product {rental_id} with data from main product {main_product_id}")
                    else:
                        logger.warning(f"Main product {main_product_id} not found for rental product {rental_id}")
                
                conn.commit()
                logger.info(f"Successfully enriched {affected} rental products")
                return affected
                
        except Exception as e:
            logger.error(f"Error enriching rental products: {str(e)}")
            raise

    def get_rental_products_status(self) -> Dict[str, Any]:
        """
        Get status of rental products and their enrichment status from WortmannProdukte_backup.
        Returns information about which rental products need enrichment.
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get all rental products
                cursor.execute("""
                    SELECT ProductId, Title, 
                           CASE WHEN LongDescription IS NULL OR LongDescription = '' THEN 'Missing' ELSE 'Present' END as LongDescription_status,
                           CASE WHEN ImagePrimary IS NULL OR ImagePrimary = '' THEN 'Missing' ELSE 'Present' END as ImagePrimary_status,
                           CASE WHEN AccessoryProducts IS NULL OR AccessoryProducts = '' THEN 'Missing' ELSE 'Present' END as AccessoryProducts_status
                    FROM WortmannProdukte_backup 
                    WHERE ProductId LIKE '%C12' OR ProductId LIKE '%C24' OR ProductId LIKE '%C36'
                    ORDER BY ProductId
                """)
                
                rental_products = cursor.fetchall()
                
                # Count status
                total_rental = len(rental_products)
                missing_long_desc = sum(1 for p in rental_products if p[2] == 'Missing')
                missing_images = sum(1 for p in rental_products if p[3] == 'Missing')
                missing_accessories = sum(1 for p in rental_products if p[4] == 'Missing')
                
                return {
                    "total_rental_products": total_rental,
                    "missing_long_description": missing_long_desc,
                    "missing_images": missing_images,
                    "missing_accessories": missing_accessories,
                    "products": [
                        {
                            "product_id": p[0],
                            "title": p[1],
                            "long_description": p[2],
                            "image_primary": p[3],
                            "accessory_products": p[4]
                        } for p in rental_products
                    ]
                }
                
        except Exception as e:
            logger.error(f"Error getting rental products status: {str(e)}")
            raise

    def test_connection(self) -> bool:
        """Test database connection"""
        return self.db_manager.test_connection()


database_service = DatabaseService()
