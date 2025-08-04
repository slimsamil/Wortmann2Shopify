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
    
    def test_connection(self) -> bool:
        """Test database connection"""
        return self.db_manager.test_connection()


database_service = DatabaseService()
