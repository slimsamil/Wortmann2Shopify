import io
import zipfile
import csv
from typing import List, Dict, Any
from app.core.config import settings
from app.services.database_service import DatabaseService
import logging
import ftplib


logger = logging.getLogger(__name__)


class WortmannService:
    def __init__(self, db_service: DatabaseService | None = None):
        self.host = settings.wortmann_ftp_host
        self.port = settings.wortmann_ftp_port
        self.user = settings.wortmann_ftp_user
        self.password = settings.wortmann_ftp_password
        self.path_productcatalog = settings.wortmann_path_productcatalog
        self.path_content = settings.wortmann_path_content
        self.path_images_zip = settings.wortmann_path_images_zip
        self.db = db_service or DatabaseService()

    def _ftp_download(self, remote_path: str) -> bytes:
        with ftplib.FTP() as ftp:
            ftp.connect(self.host, self.port)
            ftp.login(self.user, self.password)
            logger.info(f"Downloading {remote_path} from Wortmann FTP...")
            bio = io.BytesIO()
            ftp.retrbinary(f"RETR {remote_path}", bio.write)
            return bio.getvalue()

    def _parse_csv(self, content: bytes) -> List[Dict[str, Any]]:
        text = content.decode('utf-8', errors='ignore')
        reader = csv.DictReader(io.StringIO(text), delimiter=';')
        rows: List[Dict[str, Any]] = []
        for row in reader:
            rows.append(row)
        return rows

    def _filter_categories(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        allowed = set([
            "ALL-IN-ONE", "Arbeitsspeicher", "Ausgabe Kabel", "Ausgabe Zubehör",
            "Ausgabegeräte", "Betriebssysteme", "CLOUD_HARDWARE", "Desktops & Server",
            "Dockingstations", "Drucker", "Eingabe Adapter", "Festplatten & SSD",
            "FIREWALL", "Flash-Speicher", "Grafikkarten", "Headset & Mikro",
            "Hubs & Switches", "Lautsprecher", "LCD", "LCD Messeware", "Mäuse",
            "Medien", "MOBILE", "Monitore", "Multifunktionsgeräte",
            "NAS Netzwerkspeicher", "Netzteil / Akku", "Netzteile & USVs", "Notebooks",
            "PAD", "PC", "PC- & Netzwerkkameras", "Prozessoren", "Scanner", "SERVER",
            "System- & Stromkabel", "Tablets", "Taschen", "Tastaturen",
            "TV-Flachbildschirme", "Verbrauchsmaterial"
        ])
        return [r for r in rows if (r.get('CategoryName_1031_German') in allowed)]

    def _combine_product_content(self, product_rows: List[Dict[str, Any]], content_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        by_id = {r.get('ProductId'): r for r in product_rows}
        combined: List[Dict[str, Any]] = []
        for c in content_rows:
            pid = c.get('ProductId')
            p = by_id.get(pid)
            if not p:
                continue
            merged = {**p, **c}
            combined.append(merged)
        return combined

    def _normalize_products(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        def parse_float(val: Any) -> float:
            s = (val or "0").replace(',', '.')
            try:
                return float(s)
            except Exception:
                return 0.0

        def parse_int(val: Any) -> int:
            try:
                return int(str(val or "0").strip())
            except Exception:
                return 0

        def to_bool_01(val: Any) -> bool:
            return str(val).strip() == '1'

        def parse_date(date_string):
            """
            Convert date from 'YYYY-MM-DD' format to 'DD.MM.YYYY' format
            
            Args:
                date_string (str): Date in format "2025-09-29"
            
            Returns:
                str: Date in format "29.09.2025"
            """

            if date_string is None:
                return '';
            if not date_string or not date_string.strip():
                return '';
                
            # Split the date string by hyphen
            year, month, day = date_string.strip().split('-')
            
            # Return in DD.MM.YYYY format
            return f"{day}.{month}.{year}"

        # Category to warranty group mapping    
        category_map = {
                    'mobile': 1,
                    'pad': 2,
                    'pc': 3,
                    'all-in-one': 4,
                    'lcd': 5,
                    'server': 6,
                    'nas': 7,
                }

        normalized: List[Dict[str, Any]] = []
        for src in items:
            # Skip products without a title
            title = src.get('Description_1031_German') or src.get('Description_1033_English')
            if not title:
                continue
                
            category_key = (src.get('Category') or '').lower()

            normalized.append({
                'ProductId': (src.get('ProductId') or '').strip(),
                'Title': title,
                'DescriptionShort': src.get('Description_1031_German') or '',
                'LongDescription': src.get('LongDescription_1031_German') or '',
                'Manufacturer': src.get('Manufacturer') or 'WORTMANN AG',
                'Category': src.get('CategoryName_1031_German') or '',
                'CategoryPath': src.get('CategoryPath_1031_German') or '',
                'Warranty': src.get('WarrantyDescription_1031_German') or 'Standard',
                'Price_B2B_Regular': parse_float(src.get('Price_B2B_Regular')),
                'Price_B2B_Discounted': parse_float(src.get('Price_B2B_Discounted')),
                'Price_B2C_inclVAT': parse_float(src.get('Price_B2C_inclVAT')),
                'Currency': src.get('Price_B2X_Currency') or 'EUR',
                'VATRate': parse_float(src.get('Price_B2C_VATRate')),
                'Stock': parse_int(src.get('Stock')),
                'StockNextDelivery': parse_date(src.get('StockNextDelivery')) or '',
                'ImagePrimary': src.get('ImagePrimary') or '',
                'ImageAdditional': src.get('ImageAdditional') or '',
                'GrossWeight': parse_float(src.get('GrossWeight')),
                'NetWeight': parse_float(src.get('NetWeight')),
                'NonReturnable': to_bool_01(src.get('NonReturnable')),
                'EOL': to_bool_01(src.get('EOL')),
                'Promotion': to_bool_01(src.get('Promotion')),
                'AccessoryProducts': src.get('AccessoryProducts') or '',
                'Garantiegruppe': category_map.get(category_key, 0),
            })
            
        return normalized

    def _extract_images_from_zip(self, zip_bytes: bytes) -> Dict[str, bytes]:
        out: Dict[str, bytes] = {}
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for name in zf.namelist():
                if name.endswith('/'):
                    continue
                try:
                    out[name.split('/')[-1]] = zf.read(name)
                except Exception:
                    continue
        return out

    def _enrich_rental_products(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enrich rental products (C12/C24/C36) with data from their main products.
        This happens before database insertion for better performance.
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
            main_products_count = 0
            
            for product in products:
                product_id = product.get('ProductId', '')
                
                # Check if this is a rental product that needs enrichment
                if (product_id.endswith('C12') or product_id.endswith('C24') or product_id.endswith('C36')):
                    main_product_id = product_id[:-3]
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
                        
                        logger.info(f"Enriched rental product {product_id} with data from main product {main_product_id}")
                    else:
                        # Keep original if no main product found
                        enriched_products.append(product)
                        logger.warning(f"Main product {main_product_id} not found for rental product {product_id}")
                else:
                    # Keep main products as-is
                    enriched_products.append(product)
                    main_products_count += 1
            
            logger.info(f"Enriched {enriched_count} rental products and kept {main_products_count} main products during FTP processing")
            logger.info(f"Total products to be inserted: {len(enriched_products)} (input: {len(products)})")
            return enriched_products
            
        except Exception as e:
            logger.error(f"Error enriching rental products: {str(e)}")
            raise

    def _expand_image_rows(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for p in products:
            pid = p.get('ProductId')
            primary = p.get('ImagePrimary')
            additional = p.get('ImageAdditional') or ''
            filenames = []
            if primary:
                filenames.append(primary)
            if additional:
                filenames.extend([f for f in additional.split('|') if f])
            for fn in filenames:
                rows.append({
                    'supplier_aid': pid,
                    'filename': fn,
                    'IsPrimary': 1 if (primary and fn == primary) else 0,
                })
        return rows

    def run_import(self) -> Dict[str, Any]:
        # Download files
        prod_csv = self._ftp_download(self.path_productcatalog)
        content_csv = self._ftp_download(self.path_content)
        images_zip = self._ftp_download(self.path_images_zip)

        # Parse CSVs
        products_raw = self._parse_csv(prod_csv)
        products_filtered = self._filter_categories(products_raw)
        content_rows = self._parse_csv(content_csv)
        combined = self._combine_product_content(products_filtered, content_rows)
        normalized = self._normalize_products(combined)

        # Enrich rental products with main product data before database insertion
        enriched_products = self._enrich_rental_products(normalized)
        
        # Upsert products (now with enriched rental products)
        upserted = self.db.upsert_wortmann_products(enriched_products)

        # Prepare images
        zip_map = self._extract_images_from_zip(images_zip)
        image_rows = self._expand_image_rows(enriched_products)
        to_insert: List[Dict[str, Any]] = []
        for row in image_rows:
            data = zip_map.get(row['filename'])
            if not data:
                continue
            to_insert.append({
                'supplier_aid': row['supplier_aid'],
                'filename': row['filename'],
                'data': data,
                'IsPrimary': row['IsPrimary'],
            })

        inserted_images = self.db.insert_images_records(to_insert)

        return {
            'products_upserted': upserted,
            'images_inserted': inserted_images,
            'products_processed': len(enriched_products),
            'images_candidates': len(image_rows),
        }


wortmann_service = WortmannService()




