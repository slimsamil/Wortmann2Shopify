from app.services.product_service import ProductService


def test_merge_and_process_simple():
    svc = ProductService()
    products = [{
        "ProductId": "123",
        "Title": "Title",
        "Price_B2C_inclVAT": 10.0,
        "Stock": 3,
        "GrossWeight": 1.0,
        "Warranty": "Standard",
        "Garantiegruppe": 0,
    }]
    images = [{"supplier_aid": "123", "base64": "aGVsbG8=", "IsPrimary": 1}]
    warranties = []

    merged = svc.merge_data(products, images, warranties)
    assert merged and len(merged) >= 1

    wrappers = svc.process_products(merged)
    assert wrappers and wrappers[0].product.handle == "prod-123"
    p = wrappers[0].product
    assert p.variants and len(p.variants) == 1


