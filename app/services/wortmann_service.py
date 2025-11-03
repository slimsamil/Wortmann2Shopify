import io
import zipfile
import csv
from typing import List, Dict, Any
from app.core.config import settings
from app.services.database_service import DatabaseService
import logging
import ftplib
import re
import html as _html


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
        # Precompiled patterns for CPU/GPU detection
        CPU_REGEX = re.compile(
            r"(Intel\s+(Core\s+i[3579]|Pentium|Celeron)\s*-?\s*\d{3,5}[A-Z]{0,3})|"
            r"(Intel\s+Core\s+Ultra\s*\d{1,3}[A-Z]?)|"
            r"(Intel\s+N\d{3,4}[A-Z]?)|"
            r"(Intel\s+Xeon\s+[A-Za-z0-9\-]+)|"
            r"(AMD\s+(Ryzen\s+\d|Athlon)\s*[A-Z]?\s*\d{3,5}[A-Z]{0,3})|"
            r"(Apple\s+M[1-5](?:\s+(Pro|Max|Ultra))?)",
            flags=re.IGNORECASE
        )

        GPU_REGEX = re.compile(
            r"(NVIDIA\s+GeForce\s+RTX?\s*\d{3,4}(?:\s*(Ti|SUPER))?)|"
            r"(AMD\s+Radeon\s+(RX|Pro)\s*\w*\s*\d{3,4}\w*)|"
            r"(Intel\s+(Iris\s+Xe|Arc\s+\w+|UHD\s+Graphics))",
            flags=re.IGNORECASE
        )

        def _strip_parentheses(text: str) -> str:
            t = _html.unescape(text)
            return re.sub(r"\([^\)]*\)", " ", t)

        def _clean_cpu(text: str) -> str:
            if not text:
                return ''
            t = _html.unescape(str(text))
            # remove trademark symbols
            t = t.replace('®', '').replace('™', '').replace('©', '')
            # cut at separators
            t = re.split(r"\s*@|\s*GHz|\s*bis zu|/|;|\s+mit\s+|\(|,|\bSmart\s*Cache\b|\bIntel\s*Smart\s*Cache\b|\bCache\b", t, maxsplit=1, flags=re.IGNORECASE)[0]
            t = _strip_parentheses(t)
            # remove add-ons
            t = re.sub(r"\b(Kerne|Threads|Generation|CPU|Prozessor|Smart\s*Cache|Cache)\b", " ", t, flags=re.IGNORECASE)
            # remove frequency/turbo words
            t = re.sub(r"\b\d+(?:[\.,]\d+)?\s*GHz\b", " ", t, flags=re.IGNORECASE)
            t = re.sub(r"\b(Turbo|Boost)\b[\w\s\.-]*", " ", t, flags=re.IGNORECASE)
            # brand casing
            t = re.sub(r"\bintel\b", "Intel", t, flags=re.IGNORECASE)
            t = re.sub(r"\bamd\b", "AMD", t, flags=re.IGNORECASE)
            t = re.sub(r"\bapple\b", "Apple", t, flags=re.IGNORECASE)
            # Normalize spacing in model name parts like 'i5 12400' and 'i5-12400'
            t = re.sub(r"\b(Core\s+i[3579])\s*(\d)", r"\1-\2", t, flags=re.IGNORECASE)
            t = re.sub(r"\s+", " ", t).strip(" -")
            return t

        def _clean_gpu(text: str) -> str:
            if not text:
                return ''
            t = str(text)
            # cut after connectors and remove VRAM blocks
            t = re.split(r"\s+mit\s+|/|;", t, maxsplit=1, flags=re.IGNORECASE)[0]
            t = re.sub(r"\b(\d+\s*GB\b.*)$", " ", t, flags=re.IGNORECASE)
            # remove any parenthetical content
            t = re.sub(r"\([^\)]*\)", " ", t)
            # remove suffixes
            t = re.sub(r"\b(Laptop\s*GPU|Mobile|Max-?Q)\b", " ", t, flags=re.IGNORECASE)
            # remove duplicate brands
            t = re.sub(r"\b(NVIDIA)(?:\s+NVIDIA)+\b", r"\1", t, flags=re.IGNORECASE)
            # brand casing
            t = re.sub(r"\bnvidia\b", "NVIDIA", t, flags=re.IGNORECASE)
            t = re.sub(r"\bamd\b", "AMD", t, flags=re.IGNORECASE)
            t = re.sub(r"\bintel\b", "Intel", t, flags=re.IGNORECASE)
            # standardize Intel Iris Xe Graphics -> Intel Iris Xe
            t = re.sub(r"\bIntel\s+Iris\s+Xe\s+Graphics\b", "Intel Iris Xe", t, flags=re.IGNORECASE)
            t = re.sub(r"\s+", " ", t).strip(" -")
            return t

        def _extract_cpu_gpu_from_text(html: str) -> Dict[str, str]:
            out: Dict[str, str] = {}
            if not html:
                return out
            # plain text for searching
            text = re.sub(r"<[^>]+>", " ", html)
            text = _html.unescape(text)
            text = text.replace('®', '').replace('™', '').replace('©', '')
            text = re.sub(r"\s+", " ", text)

            m_cpu = CPU_REGEX.search(text)
            if m_cpu:
                out['cpu'] = _clean_cpu(m_cpu.group(0))
            m_gpu = GPU_REGEX.search(text)
            if m_gpu:
                out['gpu'] = _clean_gpu(m_gpu.group(0))
            return out

        def _clean_text(text: str) -> str:
            if text is None:
                return ''
            # Collapse whitespace and strip
            return re.sub(r"\s+", " ", str(text)).strip()

        def _strip_html(text: str) -> str:
            if not text:
                return ''
            # Remove tags and unescape basic entities
            no_tags = re.sub(r"<[^>]+>", " ", text)
            no_tags = _html.unescape(no_tags)
            return _clean_text(no_tags)

        def _normalize_diagonal(text: str) -> str:
            if not text:
                return ''
            s = _strip_html(text)
            # Prefer explicit inch markers first
            m_in = re.search(r"(\d+[\.,]?\d*)\s*(?:Zoll|\")", s, flags=re.IGNORECASE)
            if m_in:
                val = m_in.group(1).replace(',', '.')
                # remove trailing .0
                try:
                    f = float(val)
                    if abs(f - int(f)) < 1e-6:
                        val = str(int(f))
                    else:
                        # keep one decimal (e.g., 15.6)
                        val = ("%.1f" % f).rstrip('0').rstrip('.')
                except Exception:
                    pass
                return f"{val}\""
            # Fallback: cm value, possibly with inch in parentheses
            # Case like: 68.6 cm (27")
            m_paren_in = re.search(r"\((\d+[\.,]?\d*)\s*\"\)", s)
            if m_paren_in:
                val = m_paren_in.group(1).replace(',', '.')
                try:
                    f = float(val)
                    if abs(f - int(f)) < 1e-6:
                        val = str(int(f))
                    else:
                        val = ("%.1f" % f).rstrip('0').rstrip('.')
                except Exception:
                    pass
                return f"{val}\""
            # Convert cm -> inch
            m_cm = re.search(r"(\d+[\.,]?\d*)\s*cm", s, flags=re.IGNORECASE)
            if m_cm:
                val = m_cm.group(1).replace(',', '.')
                try:
                    f_cm = float(val)
                    f_in = f_cm / 2.54
                    # round to nearest 0.1 to keep values like 15.6
                    f_in = round(f_in, 1)
                    if abs(f_in - int(f_in)) < 1e-6:
                        val_in = str(int(f_in))
                    else:
                        val_in = ("%.1f" % f_in).rstrip('0').rstrip('.')
                    return f"{val_in}\""
                except Exception:
                    return ''
            return ''

        def _extract_specs_from_html(html: str) -> Dict[str, Any]:
            if not html:
                return {}
            out: Dict[str, Any] = {}
            
            def _capacity(text: str) -> str:
                if not text:
                    return ''
                # find all occurrences like 1 TB, 1000 GB, 16 GB, 512 MB etc.
                matches = re.findall(r"(\d+[\.,]?\d*)\s*(TB|GB|MB)", text, flags=re.IGNORECASE)
                if not matches:
                    return text.strip()
                # prefer TB > GB > MB; if multiple, pick the first highest unit
                rank = {"TB": 3, "GB": 2, "MB": 1}
                # normalize to upper
                ranked = sorted(((float(v.replace(',', '.')), u.upper()) for v, u in matches), key=lambda x: -rank[x[1]])
                value, unit = ranked[0]
                # Keep original integer if whole
                if abs(value - int(value)) < 1e-6:
                    value_str = str(int(value))
                else:
                    value_str = f"{value:.1f}".rstrip('0').rstrip('.')
                return f"{value_str} {unit}"
            try:
                # Find table rows and their <td> cells
                for row in re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE | re.DOTALL):
                    cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, flags=re.IGNORECASE | re.DOTALL)
                    if not cells:
                        continue
                    label = _strip_html(cells[0]).lower()
                    value = _strip_html(cells[1]) if len(cells) > 1 else ''
                    if not label:
                        continue

                    # Map label synonyms to target keys
                    def set_if_empty(key: str, val: str):
                        if key not in out and val:
                            out[key] = val

                    # Bildschirmdiagonale (LCD)
                    if any(k in label for k in ["bildschirmdiagonale", "display-diagonale", "displaydiagonale", "schirmgröße"]):
                        set_if_empty("Bildschirmdiagonale", _normalize_diagonal(value))

                    # Prozessor (PC/Mobile)
                    if any(k in label for k in ["prozessor", "cpu", "prozessor-modell", "prozessormodell", "prozessorbezeichnung"]):
                        set_if_empty("Prozessor", value)

                    # Prozessorfamilie (Server)
                    if any(k in label for k in ["prozessorfamilie", "cpu-familie", "prozessor familie"]):
                        set_if_empty("Prozessorfamilie", value)

                    # GPU (Grafikkarte)
                    if any(k in label for k in [
                        "grafikkarte", "grafikkarte-familie", "grafikkartenfamilie",
                        "eingebautes grafikkartenmodell", "gpu", "grafikprozessor"
                    ]):
                        set_if_empty("GPU", value)

                    # RAM (installed), ignore maximum indicators; allow 'speicherkapazität' (RAM context)
                    if ("arbeitsspeicher" in label or re.search(r"\bram\b", label) or "speicherkapazität" in label or "speicherlayout" in label):
                        if not re.search(r"\bmax\b|maximal", label) and not any(x in label for x in ["gesamtspeicher", "ssd", "hdd", "festplatte"]):
                            if "speicherlayout" in label and "RAM" in out:
                                pass
                            else:
                                set_if_empty("RAM", _capacity(value))

                    # Speicher (total storage): only from explicit total capacity labels
                    if any(k in label for k in [
                        "gesamtspeicherkapazität", "gesamtspeicher"
                    ]):
                        set_if_empty("Speicher", _capacity(value))
            except Exception:
                # Best-effort parsing; ignore errors
                pass
            return out

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


        normalized: List[Dict[str, Any]] = []
        for src in items:
            # Skip products without a title
            title = src.get('Description_1031_German') or src.get('Description_1033_English')
            if not title:
                continue
                

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
                'AccessoryProducts': src.get('AccessoryProducts'),
            })

        category_map = {
            'mobile': 1,
            'pad': 2,
            'pc': 3,
            'all-in-one': 4,
            'lcd': 5,
            'server': 6,
            'nas': 7,
        }

        for n in normalized:
            key = (n.get('Category') or '').lower()
            n['Garantiegruppe'] = category_map.get(key, 0)

            # Extract specs from LongDescription and assign fields
            long_html = n.get('LongDescription') or ''
            specs = _extract_specs_from_html(long_html)
            # Fallback: detect CPU/GPU from raw text if table parsing didn't yield values
            aux = _extract_cpu_gpu_from_text(long_html)
            if 'Prozessor' not in specs and aux.get('cpu'):
                specs['Prozessor'] = aux['cpu']
            if 'GPU' not in specs and aux.get('gpu'):
                specs['GPU'] = aux['gpu']
            if 'Prozessorfamilie' not in specs and aux.get('cpu'):
                # Extract family from CPU for Prozessorfamilie if not found in table
                cpu_text = aux['cpu'].lower()
                if 'core i' in cpu_text:
                    specs['Prozessorfamilie'] = 'Core i5' if 'i5' in cpu_text else 'Core i3' if 'i3' in cpu_text else 'Core i7' if 'i7' in cpu_text else 'Core i9' if 'i9' in cpu_text else 'Core i'
                elif 'core ultra' in cpu_text:
                    specs['Prozessorfamilie'] = 'Core Ultra'
                elif 'ryzen' in cpu_text:
                    specs['Prozessorfamilie'] = 'Ryzen'
                elif 'athlon' in cpu_text:
                    specs['Prozessorfamilie'] = 'Athlon'
                elif any(m in cpu_text for m in ['m1', 'm2', 'm3', 'm4', 'm5']):
                    specs['Prozessorfamilie'] = 'M Series'
            
            # Fallback: detect Bildschirmdiagonale from raw text if table parsing didn't yield values
            if 'Bildschirmdiagonale' not in specs or not specs.get('Bildschirmdiagonale'):
                diagonal_from_text = _normalize_diagonal(long_html)
                if diagonal_from_text:
                    specs['Bildschirmdiagonale'] = diagonal_from_text

            # Default to None if missing as requested
            n['Bildschirmdiagonale'] = specs.get('Bildschirmdiagonale') if specs.get('Bildschirmdiagonale') else None
            # Apply final cleaning on CPU/GPU according to rules
            cpu_raw = specs.get('Prozessor') or ''
            gpu_raw = specs.get('GPU') or ''
            cpu_clean = _clean_cpu(cpu_raw) if cpu_raw else ''
            gpu_clean = _clean_gpu(gpu_raw) if gpu_raw else ''

            n['Prozessor'] = cpu_clean if cpu_clean else None
            n['GPU'] = gpu_clean if gpu_clean else None
            n['RAM'] = specs.get('RAM') if specs.get('RAM') else None
            n['Speicher'] = specs.get('Speicher') if specs.get('Speicher') else None
            
            # Normalize Prozessorfamilie: add brand if missing
            prozessorfamilie_raw = specs.get('Prozessorfamilie') or ''
            if prozessorfamilie_raw:
                prozessorfamilie_lower = prozessorfamilie_raw.lower()
                # Check if brand is already present
                if not any(brand in prozessorfamilie_lower for brand in ['intel', 'amd', 'apple']):
                    # Add appropriate brand based on model name
                    if any(model in prozessorfamilie_lower for model in ['core i', 'core ultra', 'pentium', 'celeron', 'xeon', 'n100', 'n200']):
                        prozessorfamilie_raw = f"Intel {prozessorfamilie_raw}"
                    elif any(model in prozessorfamilie_lower for model in ['ryzen', 'athlon']):
                        prozessorfamilie_raw = f"AMD {prozessorfamilie_raw}"
                    elif any(model in prozessorfamilie_lower for model in ['m1', 'm2', 'm3', 'm4', 'm5']):
                        prozessorfamilie_raw = f"Apple {prozessorfamilie_raw}"
                n['Prozessorfamilie'] = prozessorfamilie_raw
            else:
                n['Prozessorfamilie'] = None
        
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
                        # Also copy the extracted metafields from main product
                        enriched_product['Bildschirmdiagonale'] = main_product.get('Bildschirmdiagonale')
                        enriched_product['Prozessor'] = main_product.get('Prozessor')
                        enriched_product['GPU'] = main_product.get('GPU')
                        enriched_product['RAM'] = main_product.get('RAM')
                        enriched_product['Speicher'] = main_product.get('Speicher')
                        enriched_product['Prozessorfamilie'] = main_product.get('Prozessorfamilie')
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




