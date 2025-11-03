from typing import List, Dict, Any
from app.core.database import db_manager
import logging

logger = logging.getLogger(__name__)


class DatabaseService:
    def __init__(self):
        self.db_manager = db_manager
    
    def fetch_products(self, limit: int = None) -> List[Dict[str, Any]]:
        """Fetch products from WortmannProdukte table, excluding EOL products"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                if limit is not None:
                    cursor.execute(f"SELECT TOP({limit}) * FROM WortmannProdukte WHERE EOL = 0")
                else:
                    cursor.execute("SELECT * FROM WortmannProdukte WHERE EOL = 0")
                
                columns = [column[0] for column in cursor.description]
                products = []
                
                for row in cursor.fetchall():
                    product_dict = {}
                    for i, value in enumerate(row):
                        product_dict[columns[i]] = value
                    products.append(product_dict)
                
                logger.info(f"Fetched {len(products)} products from database (excluding EOL)")
                return products
        except Exception as e:
            logger.error(f"Error fetching products: {str(e)}")
            raise
    
    
    def fetch_products_by_ids(self, product_ids: List[str]) -> List[Dict[str, Any]]:
        """Fetch multiple products by ProductId list in a single query from WortmannProdukte"""
        if not product_ids:
            return []
        
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Create placeholders for IN clause
                placeholders = ','.join(['?' for _ in product_ids])
                query = f"SELECT * FROM WortmannProdukte WHERE ProductId IN ({placeholders})"
                
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
        """Upsert Wortmann product rows into WortmannProdukte table."""
        if not products:
            return 0
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                affected = 0
                
                # Insert/Update all products (rental products should already be enriched)
                for p in products:
                    # Delete existing row for ProductId to avoid duplicates
                    cursor.execute("DELETE FROM WortmannProdukte WHERE ProductId = ?", (p.get('ProductId'),))
                    cursor.execute(
                        """
                        INSERT INTO WortmannProdukte (
                            ProductId, Title, DescriptionShort, LongDescription,
                            Manufacturer, Category, CategoryPath, Warranty,
                            Price_B2B_Regular, Price_B2B_Discounted, Price_B2C_inclVAT,
                            Currency, VATRate, Stock, StockNextDelivery,
                            ImagePrimary, ImageAdditional, GrossWeight, NetWeight,
                                NonReturnable, EOL, Promotion, Garantiegruppe, AccessoryProducts,
                                Bildschirmdiagonale, Prozessor, GPU, RAM, Speicher, Prozessorfamilie
                            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            p.get('ProductId'), p.get('Title'), p.get('DescriptionShort'), p.get('LongDescription'),
                            p.get('Manufacturer'), p.get('Category'), p.get('CategoryPath'), p.get('Warranty'),
                            p.get('Price_B2B_Regular'), p.get('Price_B2B_Discounted'), p.get('Price_B2C_inclVAT'),
                            p.get('Currency'), p.get('VATRate'), p.get('Stock'), p.get('StockNextDelivery'),
                            p.get('ImagePrimary'), p.get('ImageAdditional'), p.get('GrossWeight'), p.get('NetWeight'),
                                1 if p.get('NonReturnable') else 0, 1 if p.get('EOL') else 0, 1 if p.get('Promotion') else 0,
                                p.get('Garantiegruppe'), p.get('AccessoryProducts'),
                                p.get('Bildschirmdiagonale'), p.get('Prozessor'), p.get('GPU'), p.get('RAM'), p.get('Speicher'), p.get('Prozessorfamilie')
                        )
                    )
                    affected += 1
                
                conn.commit()
                logger.info(f"Upserted {affected} Wortmann products")
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
    
database_service = DatabaseService()
