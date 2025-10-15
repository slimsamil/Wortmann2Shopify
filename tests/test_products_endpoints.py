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


