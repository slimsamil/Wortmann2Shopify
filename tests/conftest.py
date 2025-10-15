import asyncio
import os
import sys
import pytest
from fastapi.testclient import TestClient

# Ensure project root is on sys.path for `import app`
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.main import app
from app.api import deps


class _FakeDB:
    def test_connection(self) -> bool:
        return True

    # Minimal stubs used by endpoints
    def fetch_products(self, limit=None):
        return [{"ProductId": "123", "Title": "T", "Price_B2C_inclVAT": 10.0, "Stock": 5, "GrossWeight": 1.2}]

    def fetch_images(self, limit=None):
        return [{"supplier_aid": "123", "base64": "aGVsbG8=", "IsPrimary": 1}]

    def fetch_warranties(self):
        return [{"id": 1, "name": "Std", "monate": 12, "prozentsatz": 0, "garantiegruppe": 0}]

    def fetch_products_by_ids(self, ids):
        return [{"ProductId": ids[0], "Title": "T", "Price_B2C_inclVAT": 10.0, "Stock": 5, "GrossWeight": 1.2}]

    def fetch_images_by_supplier_aids(self, aids):
        return []

    def fetch_warranties_by_groups(self, groups):
        return []


class _FakeShopify:
    async def test_connection(self) -> bool:
        return True

    async def send_products_batch(self, wrappers, batch_size):
        return [{"status": "success"} for _ in wrappers]

    async def fetch_all_product_handles_bulk(self):
        return [{"id": "gid://shopify/Product/111", "handle": "prod-123"}]

    async def delete_product_by_id(self, sid: int):
        return {"status": "success"}

    async def get_product_by_handle(self, handle: str):
        return {"id": 111, "handle": handle}

    async def update_product_by_handle(self, handle: str, data: dict):
        return {"status": "success", "handle": handle}


@pytest.fixture(autouse=True)
def _override_dependencies():
    app.dependency_overrides[deps.get_database_service] = lambda: _FakeDB()
    app.dependency_overrides[deps.get_shopify_service] = lambda: _FakeShopify()
    yield
    app.dependency_overrides.clear()


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture()
def client():
    return TestClient(app)


