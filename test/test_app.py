__version__ = "v3"

import fastapi
import pytest
from fastapi.testclient import TestClient

import app


# Create a generator that when called gives
# a mock/ test API client, for our FastAPI app.
@pytest.fixture
def client():
    # Get a mock FastAPI app configured exactly like the real one (CORS,
    # logging middleware, and all /v3, /latest, and unversioned routes).
    mock_app = fastapi.FastAPI()
    app.configure(mock_app)

    # Yield the mock/ test client for the FastAPI
    # app we spun up any time this generator is called.
    with TestClient(mock_app) as client:
        yield client


# Test for the root endpoint
def test_read_main(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"Hello": "World"}


def test_health_endpoint(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_endpoint(client):
    # /ready runs a SELECT 1 against the configured DB. The rest of this
    # test suite requires the DB to be up, so we assert the success path.
    response = client.get("/ready")
    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_v4_root_endpoint(client):
    # The /v4 discovery root reports the preview status of the opt-in v4
    # surface (epic #842, #824). Both the trailing-slash and bare forms are
    # served directly with no 307 redirect (follow_redirects=False) so health
    # checks and strict clients can hit either.
    for path in ("/v4/", "/v4"):
        response = client.get(path, follow_redirects=False)
        assert response.status_code == 200, path
        assert response.json() == {"version": "v4", "status": "preview"}


def test_ready_endpoint_returns_503_when_db_unreachable(client, monkeypatch):
    # Force the engine.connect() call inside /ready to raise so we exercise
    # the failure path and confirm the 503 contract.
    def boom(*args, **kwargs):
        raise RuntimeError("simulated DB outage")

    monkeypatch.setattr(app, "async_engine", type("X", (), {"connect": boom})())
    response = client.get("/ready")
    assert response.status_code == 503
    assert response.json() == {"status": "unavailable"}
