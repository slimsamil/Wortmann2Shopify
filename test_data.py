#!/usr/bin/env python3
"""
Test Data Generator for Shopify API
This script creates test data for 10 products to test the update function.
"""

import base64
import json
from typing import List, Dict, Any
import pyodbc
from app.core.config import settings


def create_test_products() -> List[Dict[str, Any]]:
    """Create 10 test products"""
    products = [
        {
            "ProductId": "TEST001",
            "Title": "Test Laptop Pro",
            "LongDescription": "High-performance laptop for professional use with latest Intel processor and 16GB RAM.",
            "DescriptionShort": "Professional laptop with Intel i7 processor",
            "Manufacturer": "TestTech",
            "Category": "Electronics",
            "CategoryPath": "Electronics|Computers|Laptops",
            "Price_B2C_inclVAT": 1299.99,
            "Price_B2B_Regular": 1199.99,
            "Stock": 15,
            "GrossWeight": 2.5,
            "NetWeight": 2.2,
            "Warranty": "24 Monate",
            "Garantiegruppe": 1
        },
        {
            "ProductId": "TEST002",
            "Title": "Wireless Gaming Mouse",
            "LongDescription": "Ergonomic wireless gaming mouse with RGB lighting and programmable buttons.",
            "DescriptionShort": "RGB wireless gaming mouse",
            "Manufacturer": "GameTech",
            "Category": "Gaming",
            "CategoryPath": "Electronics|Gaming|Accessories",
            "Price_B2C_inclVAT": 89.99,
            "Price_B2B_Regular": 79.99,
            "Stock": 45,
            "GrossWeight": 0.3,
            "NetWeight": 0.25,
            "Warranty": "12 Monate",
            "Garantiegruppe": 0
        },
        {
            "ProductId": "TEST003",
            "Title": "4K Gaming Monitor",
            "LongDescription": "27-inch 4K gaming monitor with 144Hz refresh rate and HDR support.",
            "DescriptionShort": "27\" 4K gaming monitor",
            "Manufacturer": "DisplayTech",
            "Category": "Monitors",
            "CategoryPath": "Electronics|Monitors|Gaming",
            "Price_B2C_inclVAT": 599.99,
            "Price_B2B_Regular": 549.99,
            "Stock": 8,
            "GrossWeight": 8.5,
            "NetWeight": 7.8,
            "Warranty": "36 Monate",
            "Garantiegruppe": 2
        },
        {
            "ProductId": "TEST004",
            "Title": "Mechanical Keyboard",
            "LongDescription": "Full-size mechanical keyboard with Cherry MX Blue switches and backlighting.",
            "DescriptionShort": "Mechanical keyboard with blue switches",
            "Manufacturer": "KeyTech",
            "Category": "Keyboards",
            "CategoryPath": "Electronics|Computers|Keyboards",
            "Price_B2C_inclVAT": 149.99,
            "Price_B2B_Regular": 129.99,
            "Stock": 22,
            "GrossWeight": 1.2,
            "NetWeight": 1.0,
            "Warranty": "24 Monate",
            "Garantiegruppe": 1
        },
        {
            "ProductId": "TEST005",
            "Title": "USB-C Hub",
            "LongDescription": "7-in-1 USB-C hub with HDMI, USB ports, SD card reader, and Ethernet.",
            "DescriptionShort": "7-in-1 USB-C hub",
            "Manufacturer": "ConnectTech",
            "Category": "Accessories",
            "CategoryPath": "Electronics|Computers|Accessories",
            "Price_B2C_inclVAT": 39.99,
            "Price_B2B_Regular": 34.99,
            "Stock": 67,
            "GrossWeight": 0.1,
            "NetWeight": 0.08,
            "Warranty": "12 Monate",
            "Garantiegruppe": 0
        },
        {
            "ProductId": "TEST006",
            "Title": "Gaming Headset",
            "LongDescription": "Wireless gaming headset with 7.1 surround sound and noise-canceling microphone.",
            "DescriptionShort": "Wireless gaming headset",
            "Manufacturer": "AudioTech",
            "Category": "Audio",
            "CategoryPath": "Electronics|Audio|Gaming",
            "Price_B2C_inclVAT": 199.99,
            "Price_B2B_Regular": 179.99,
            "Stock": 12,
            "GrossWeight": 0.8,
            "NetWeight": 0.7,
            "Warranty": "24 Monate",
            "Garantiegruppe": 1
        },
        {
            "ProductId": "TEST007",
            "Title": "SSD 1TB",
            "LongDescription": "1TB NVMe SSD with read speeds up to 3500MB/s and write speeds up to 3000MB/s.",
            "DescriptionShort": "1TB NVMe SSD",
            "Manufacturer": "StorageTech",
            "Category": "Storage",
            "CategoryPath": "Electronics|Computers|Storage",
            "Price_B2C_inclVAT": 129.99,
            "Price_B2B_Regular": 119.99,
            "Stock": 34,
            "GrossWeight": 0.05,
            "NetWeight": 0.04,
            "Warranty": "60 Monate",
            "Garantiegruppe": 3
        },
        {
            "ProductId": "TEST008",
            "Title": "Webcam HD",
            "LongDescription": "1080p HD webcam with autofocus and built-in microphone for video conferencing.",
            "DescriptionShort": "1080p HD webcam",
            "Manufacturer": "VideoTech",
            "Category": "Webcams",
            "CategoryPath": "Electronics|Computers|Webcams",
            "Price_B2C_inclVAT": 79.99,
            "Price_B2B_Regular": 69.99,
            "Stock": 28,
            "GrossWeight": 0.2,
            "NetWeight": 0.18,
            "Warranty": "12 Monate",
            "Garantiegruppe": 0
        },
        {
            "ProductId": "TEST009",
            "Title": "Gaming Chair",
            "LongDescription": "Ergonomic gaming chair with lumbar support, adjustable armrests, and breathable mesh.",
            "DescriptionShort": "Ergonomic gaming chair",
            "Manufacturer": "ChairTech",
            "Category": "Furniture",
            "CategoryPath": "Electronics|Gaming|Furniture",
            "Price_B2C_inclVAT": 299.99,
            "Price_B2B_Regular": 269.99,
            "Stock": 5,
            "GrossWeight": 25.0,
            "NetWeight": 23.5,
            "Warranty": "24 Monate",
            "Garantiegruppe": 1
        },
        {
            "ProductId": "TEST010",
            "Title": "WiFi Router",
            "LongDescription": "Dual-band WiFi 6 router with speeds up to 3000Mbps and advanced security features.",
            "DescriptionShort": "WiFi 6 router",
            "Manufacturer": "NetworkTech",
            "Category": "Networking",
            "CategoryPath": "Electronics|Networking|Routers",
            "Price_B2C_inclVAT": 159.99,
            "Price_B2B_Regular": 139.99,
            "Stock": 18,
            "GrossWeight": 0.6,
            "NetWeight": 0.55,
            "Warranty": "24 Monate",
            "Garantiegruppe": 1
        }
    ]
    return products


def create_test_images() -> List[Dict[str, Any]]:
    """Create test images for the products"""
    # Create a simple 1x1 pixel PNG image in base64
    # This is a minimal valid PNG file
    simple_png_base64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
    
    images = []
    for i in range(1, 11):
        product_id = f"TEST{i:03d}"
        images.append({
            "supplier_aid": product_id,
            "base64": simple_png_base64,
            "hex": "89504e470d0a1a0a0000000d4948445200000001000000010802000000907753de0000000b4944415478da6364f8ff1f0002000100030152b2fecd0000000049454e44ae426082"
        })
    
    return images


def create_test_warranties() -> List[Dict[str, Any]]:
    """Create test warranty options"""
    warranties = [
        {
            "id": 1,
            "name": "Standard 12 Monate",
            "monate": 12,
            "prozentsatz": 0.0,
            "minimum": 0.0,
            "garantiegruppe": 0
        },
        {
            "id": 2,
            "name": "Extended 24 Monate",
            "monate": 24,
            "prozentsatz": 5.0,
            "minimum": 10.0,
            "garantiegruppe": 1
        },
        {
            "id": 3,
            "name": "Premium 36 Monate",
            "monate": 36,
            "prozentsatz": 8.0,
            "minimum": 15.0,
            "garantiegruppe": 2
        },
        {
            "id": 4,
            "name": "Ultimate 60 Monate",
            "monate": 60,
            "prozentsatz": 12.0,
            "minimum": 25.0,
            "garantiegruppe": 3
        }
    ]
    return warranties


def insert_test_data():
    """Insert test data into the database"""
    connection_string = (
        f"DRIVER={{{settings.db_driver}}};"
        f"SERVER={settings.db_server};"
        f"DATABASE={settings.db_name};"
        f"UID={settings.db_user};"
        f"PWD={settings.db_password};"
        f"TrustServerCertificate=yes;"
    )
    
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Clear existing test data
        print("Clearing existing test data...")
        cursor.execute("DELETE FROM WortmannProdukte WHERE ProductId LIKE 'TEST%'")
        cursor.execute("DELETE FROM BilderShopify WHERE supplier_aid LIKE 'TEST%'")
        cursor.execute("DELETE FROM GarantieOptionen WHERE id > 0")
        
        # Insert products
        print("Inserting test products...")
        products = create_test_products()
        for product in products:
            cursor.execute("""
                INSERT INTO WortmannProdukte (
                    ProductId, Title, LongDescription, DescriptionShort, Manufacturer,
                    Category, CategoryPath, Price_B2C_inclVAT, Price_B2B_Regular,
                    Stock, GrossWeight, NetWeight, Warranty, Garantiegruppe
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                product["ProductId"], product["Title"], product["LongDescription"],
                product["DescriptionShort"], product["Manufacturer"], product["Category"],
                product["CategoryPath"], product["Price_B2C_inclVAT"], product["Price_B2B_Regular"],
                product["Stock"], product["GrossWeight"], product["NetWeight"],
                product["Warranty"], product["Garantiegruppe"]
            ))
        
        # Insert images
        print("Inserting test images...")
        images = create_test_images()
        for image in images:
            cursor.execute("""
                INSERT INTO BilderShopify (supplier_aid, base64, hex)
                VALUES (?, ?, ?)
            """, (image["supplier_aid"], image["base64"], image["hex"]))
        
        # Insert warranties
        print("Inserting test warranties...")
        warranties = create_test_warranties()
        for warranty in warranties:
            cursor.execute("""
                INSERT INTO GarantieOptionen (id, name, monate, prozentsatz, minimum, garantiegruppe)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                warranty["id"], warranty["name"], warranty["monate"],
                warranty["prozentsatz"], warranty["minimum"], warranty["garantiegruppe"]
            ))
        
        conn.commit()
        print(f"✅ Successfully inserted test data:")
        print(f"   - {len(products)} products")
        print(f"   - {len(images)} images")
        print(f"   - {len(warranties)} warranties")
        
    except Exception as e:
        print(f"❌ Error inserting test data: {str(e)}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    print("🧪 Shopify API Test Data Generator")
    print("=" * 40)
    insert_test_data() 