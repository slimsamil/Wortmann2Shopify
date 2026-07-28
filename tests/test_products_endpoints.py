from app.models.product import WorkflowRequest, SyncProductsRequest, DeleteProductsRequest


def test_upload_all_products_dry_run(client):
    payload = {"dry_run": True}
    r = client.post("/api/v1/products/upload-all-products", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["status"] in ("success", "completed")
    assert data["total_products"] >= 1


def test_delete_all_products(client):
    r = client.post("/api/v1/products/delete-all-products")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "completed"


def test_delete_products_by_ids(client):
    payload = DeleteProductsRequest(product_ids=["123"]).model_dump()
    r = client.post("/api/v1/products/delete-products-by-ids", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "completed"


def test_create_products_by_ids(client):
    payload = SyncProductsRequest(product_ids=["123"], batch_size=2).model_dump()
    r = client.post("/api/v1/products/create-products-by-ids", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "completed"


def test_update_products_by_ids(client):
    payload = SyncProductsRequest(product_ids=["123"], batch_size=2).model_dump()
    r = client.post("/api/v1/products/update-products-by-ids", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "completed"


def test_export_shopify_products_includes_tags(client):
    r = client.get("/api/v1/products/export-shopify-products")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 2

    by_id = {p["ProductId"]: p for p in data}

    # Tags are trimmed and empty entries dropped
    assert by_id["123"]["Tags"] == ["no-sync", "wortmann"]

    # Products without tags still carry the field, as an empty list
    assert by_id["456"]["Tags"] == []


def test_export_shopify_products_keeps_existing_fields(client):
    r = client.get("/api/v1/products/export-shopify-products")
    assert r.status_code == 200
    product = r.json()[0]

    # Tags is purely additive - the previous schema must stay intact
    for field in (
        "ProductId", "Title", "DescriptionShort", "LongDescription", "Manufacturer",
        "Category", "CategoryPath", "Warranty", "Price_B2B_Regular", "Price_B2B_Discounted",
        "Price_B2C_inclVAT", "Currency", "VATRate", "Stock", "StockNextDelivery",
        "ImagePrimary", "ImageAdditional", "GrossWeight", "NetWeight", "NonReturnable",
        "EOL", "Promotion", "AccessoryProducts", "Garantiegruppe", "RAM", "GPU",
        "Prozessor", "Prozessorfamilie", "Bildschirmdiagonale", "Speicher",
    ):
        assert field in product, f"missing field: {field}"

    assert product["Manufacturer"] == "Wortmann"
    assert product["Category"] == "Notebooks"
    assert product["Stock"] == 5


