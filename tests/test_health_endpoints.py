def test_health_check(client):
    r = client.get("/api/v1/health")
    assert r.status_code == 200
    body = r.json()
    assert body.get("status") == "healthy"


def test_test_connections(client):
    r = client.get("/api/v1/test-connections")
    assert r.status_code == 200
    body = r.json()
    assert body.get("database") == "connected"
    assert body.get("shopify") == "connected"


