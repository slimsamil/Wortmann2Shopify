import pytest
import anyio

from app.services.shopify_service import ShopifyService


@pytest.mark.anyio
async def test_test_connection_ok(monkeypatch):
    class _Resp:
        status_code = 200
        text = "{}"

    class _Client:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            return False
        async def get(self, *args, **kwargs):
            return _Resp()

    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", lambda: _Client())

    svc = ShopifyService()
    ok = await svc.test_connection()
    assert ok is True


@pytest.mark.anyio
async def test_send_products_batch_empty():
    svc = ShopifyService()
    res = await svc.send_products_batch([], batch_size=2)
    assert res == []


