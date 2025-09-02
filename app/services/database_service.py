from typing import List, Dict, Any, Optional
from app.core.database import db_manager
from app.models.product import ProductBase, ImageBase, WarrantyBase
import logging

logger = logging.getLogger(__name__)


class DatabaseService:
    def __init__(self):
        self.db_manager = db_manager
    
    def fetch_products(self, limit: int = None) -> List[Dict[str, Any]]:
        """Fetch products from WortmannProdukte table"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                if limit is not None:
                    cursor.execute(f"SELECT TOP({limit}) * FROM WortmannProdukte")
                else:
                    cursor.execute("SELECT * FROM WortmannProdukte")
                
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
    
    def fetch_product_by_id(self, product_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single product by ProductId"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM WortmannProdukte WHERE ProductId = ?", (product_id,))
                
                columns = [column[0] for column in cursor.description]
                row = cursor.fetchone()
                
                if row:
                    product_dict = {}
                    for i, value in enumerate(row):
                        product_dict[columns[i]] = value
                    
                    logger.info(f"Fetched product {product_id} from database")
                    return product_dict
                else:
                    logger.warning(f"Product {product_id} not found in database")
                    return None
                    
        except Exception as e:
            logger.error(f"Error fetching product {product_id}: {str(e)}")
            raise
    
    def fetch_test_products(self, limit: int = None) -> List[Dict[str, Any]]:
        """Fetch only test products (with TEST prefix) from WortmannProdukte table"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                if limit is not None:
                    cursor.execute(f"SELECT TOP({limit}) * FROM WortmannProdukte WHERE ProductId LIKE 'TEST%'")
                else:
                    cursor.execute("SELECT * FROM WortmannProdukte WHERE ProductId LIKE 'TEST%'")
                
                columns = [column[0] for column in cursor.description]
                products = []
                
                for row in cursor.fetchall():
                    product_dict = {}
                    for i, value in enumerate(row):
                        product_dict[columns[i]] = value
                    products.append(product_dict)
                
                logger.info(f"Fetched {len(products)} test products from database")
                return products
        except Exception as e:
            logger.error(f"Error fetching test products: {str(e)}")
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
    
    def fetch_images_by_product_id(self, product_id: str) -> List[Dict[str, Any]]:
        """Fetch images for a specific product ID from BilderShopify table"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM BilderShopify WHERE supplier_aid = ?", (product_id,))
                
                columns = [column[0] for column in cursor.description]
                images = []
                
                for row in cursor.fetchall():
                    image_dict = {}
                    for i, value in enumerate(row):
                        image_dict[columns[i]] = value
                    images.append(image_dict)
                
                logger.info(f"Fetched {len(images)} images for product {product_id} from database")
                return images
        except Exception as e:
            logger.error(f"Error fetching images for product {product_id}: {str(e)}")
            raise
    
    def fetch_test_images(self, limit: int = None) -> List[Dict[str, Any]]:
        """Fetch only test images (with TEST prefix) from BilderShopify table"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                if limit is not None:
                    cursor.execute(f"SELECT TOP({limit}) * FROM BilderShopify WHERE supplier_aid LIKE 'TEST%'")
                else:
                    cursor.execute("SELECT * FROM BilderShopify WHERE supplier_aid LIKE 'TEST%'")
                
                columns = [column[0] for column in cursor.description]
                images = []
                
                for row in cursor.fetchall():
                    image_dict = {}
                    for i, value in enumerate(row):
                        image_dict[columns[i]] = value
                    images.append(image_dict)
                
                logger.info(f"Fetched {len(images)} test images from database")
                return images
        except Exception as e:
            logger.error(f"Error fetching test images: {str(e)}")
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
    
    def upsert_wortmann_products(self, products: List[Dict[str, Any]]) -> int:
        """Upsert Wortmann product rows into WortmannProdukte table."""
        if not products:
            return 0
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                affected = 0
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
    
    def test_connection(self) -> bool:
        """Test database connection"""
        return self.db_manager.test_connection()


database_service = DatabaseService()
