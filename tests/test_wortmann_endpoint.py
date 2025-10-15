import types


def test_wortmann_import_endpoint(client, monkeypatch):
    class FakeService:
        def __init__(self, *_):
            pass

        def run_import(self):
            return {"products_upserted": 1, "images_inserted": 1, "products_processed": 1, "images_candidates": 1}

    # Patch the constructor used inside the route to return our fake
    # Patch where it's used: inside the endpoint module
    import app.api.endpoints.wortmann as wortmann_endpoint
    monkeypatch.setattr(wortmann_endpoint, "WortmannService", FakeService)

    r = client.post("/api/v1/wortmann/wortmann-import")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "completed"
    assert data["successful_uploads"] == 1


