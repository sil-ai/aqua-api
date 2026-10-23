"""Tests for refusing unknown query parameters on v4 (issue #1003).

Before #1003 an undeclared query parameter was silently dropped, so a misspelled or
renamed one (``?threshold=`` for ``?min_score=``) came back as a 200 with the unfiltered
results. Now it is a 422 shaped like an unknown body key.

Most of this runs against probe routes on a freshly built sub-app, the
``test_v4_nul_bytes.py`` pattern: no DB, no parent app. The tests at the bottom go
through the real mounted app, so "the check is on every route we actually serve" is
asserted and not assumed.
"""

import re

import fastapi
import pydantic
import pytest
from fastapi.testclient import TestClient

import app as app_module
from api_v4.app import create_v4_app
from api_v4.query_params import reject_unknown_query_params
from security_routes.v4.dependencies import get_current_user_v4


class _Scope:
    """A class dependency declaring query parameters, the shape of ``ResultScopeParams``."""

    def __init__(self, book: str | None = None, chapter: int | None = None):
        self.book = book
        self.chapter = chapter


def _deny_without_token(request: fastapi.Request) -> None:
    """Stands in for ``get_current_user_v4``: a 401 unless a token header is present."""
    if "authorization" not in request.headers:
        raise fastapi.HTTPException(status_code=401, detail="Not authenticated")


@pytest.fixture
def probe_app():
    """A fresh /v4 sub-app carrying one probe router wired the way app.py wires one."""
    v4_app = create_v4_app(configure_cors=app_module.configure_cors)
    router = fastapi.APIRouter()

    @router.get("/probe")
    def _get(
        name: str = "x",
        min_score: float | None = None,
        tags: list[str] = fastapi.Query(default=[]),
        from_: str | None = fastapi.Query(default=None, alias="from"),
        scope: _Scope = fastapi.Depends(),
    ):
        return {"ok": True, "tags": tags}

    @router.get("/probe/{key}")
    def _get_path(key: str):
        return {"ok": True}

    v4_app.include_router(
        router,
        dependencies=[
            fastapi.Depends(_deny_without_token),
            fastapi.Depends(reject_unknown_query_params),
        ],
    )
    return v4_app


@pytest.fixture
def client(probe_app):
    with TestClient(probe_app, raise_server_exceptions=False) as client:
        client.headers["authorization"] = "Bearer probe"
        yield client


def _unknown_errors(response):
    """The ``details.errors`` of an unknown-parameter 422, after checking the envelope."""
    assert response.status_code == 422, response.text
    error = response.json()["error"]
    assert error["code"] == "VALIDATION_ERROR"
    assert error["message"] == "Request validation failed."
    errors = error["details"]["errors"]
    assert {entry["type"] for entry in errors} == {"extra_forbidden"}
    return errors


def test_an_unknown_parameter_is_a_422_naming_it(client):
    """The motivating case: the old name for a filter the route now calls something else."""
    errors = _unknown_errors(client.get("/probe?threshold=0.5"))
    assert errors == [
        {
            "type": "extra_forbidden",
            "loc": ["query", "threshold"],
            "msg": "Extra inputs are not permitted",
            "input": "0.5",
        }
    ]


def test_it_matches_what_an_unknown_body_key_produces(client):
    """Same ``type`` and ``msg`` pydantic gives an unknown key under ``extra="forbid"``,
    so a client handles both with one branch. Checked against pydantic itself rather
    than a copied string."""

    class _Closed(pydantic.BaseModel):
        model_config = {"extra": "forbid"}

    with pytest.raises(pydantic.ValidationError) as excinfo:
        _Closed.model_validate({"threshold": 1})
    [body_error] = excinfo.value.errors()
    [query_error] = _unknown_errors(client.get("/probe?threshold=1"))
    assert query_error["type"] == body_error["type"]
    assert query_error["msg"] == body_error["msg"]


def test_declared_parameters_still_pass(client):
    """Including one on a sub-dependency, an aliased one by its wire name, and a list."""
    response = client.get(
        "/probe?name=a&min_score=0.5&tags=x&tags=y&from=GEN&book=MAT&chapter=1"
    )
    assert response.status_code == 200, response.text
    assert response.json()["tags"] == ["x", "y"]


def test_no_query_string_passes(client):
    assert client.get("/probe").status_code == 200


def test_an_alias_is_accepted_only_by_its_wire_name(client):
    """``from_`` is the Python name; ``from`` is what a caller sends."""
    errors = _unknown_errors(client.get("/probe?from_=GEN"))
    assert [entry["loc"] for entry in errors] == [["query", "from_"]]


def test_every_unknown_parameter_is_reported_once(client):
    """All of them in one answer, and a repeated name is one error, not two."""
    errors = _unknown_errors(client.get("/probe?a=1&name=ok&b=2&a=3"))
    assert [entry["loc"] for entry in errors] == [["query", "a"], ["query", "b"]]


def test_a_path_parameter_name_is_not_a_query_parameter(client):
    errors = _unknown_errors(client.get("/probe/k?key=k"))
    assert [entry["loc"] for entry in errors] == [["query", "key"]]


def test_a_secret_named_parameter_is_not_echoed(client):
    """The shared 422 handler's redaction (#950) applies here too, since the check
    raises the ordinary validation error rather than building its own body."""
    [error] = _unknown_errors(client.get("/probe?password=hunter2"))
    assert "hunter2" not in str(error["input"])


def test_authentication_is_still_answered_first(probe_app):
    """The check is listed after auth, so a request with no token is a 401 whatever its
    query string says, as it already was for a bad value in a declared parameter."""
    with TestClient(probe_app, raise_server_exceptions=False) as anonymous:
        assert anonymous.get("/probe?threshold=1").status_code == 401


# --- End to end, through the real mounted app ---------------------------------------


@pytest.fixture
def mounted_client():
    """The real app, built the way ``test_v4_subapp.py`` builds it: no DB needed."""
    mock_app = fastapi.FastAPI()
    app_module.configure(mock_app)
    with TestClient(mock_app, raise_server_exceptions=False) as client:
        yield client


def test_every_v4_operation_refuses_an_unknown_parameter():
    """The check is attached per router in ``create_v4_app``, so a router added there
    without it would silently drop unknown parameters again. Pinned over the whole
    published surface by calling every operation, rather than trusted.

    Auth is overridden so the check is reached, and path parameters are filled with a
    placeholder. Neither matters to the outcome: the check runs before the endpoint's
    own path, query and body parameters are validated and before the handler runs, so
    a route carrying it answers this 422 without touching the DB, and a route missing
    it answers something else.
    """
    v4_app = create_v4_app(configure_cors=app_module.configure_cors)
    v4_app.dependency_overrides[get_current_user_v4] = lambda: None
    operations = [
        (method, re.sub(r"\{[^}]+\}", "1", path))
        for path, item in v4_app.openapi()["paths"].items()
        for method in item
    ]
    assert len(operations) > 40
    with TestClient(v4_app, raise_server_exceptions=False) as anonymous:
        not_refused = []
        for method, path in operations:
            response = anonymous.request(method, f"{path}?unknown_probe=1")
            errors = (
                response.json().get("error", {}).get("details", {}).get("errors", [])
                if response.status_code == 422
                else []
            )
            if ["query", "unknown_probe"] not in [entry["loc"] for entry in errors]:
                not_refused.append((method, path, response.status_code))
    assert not_refused == []


def test_the_public_discovery_root_refuses_an_unknown_parameter(mounted_client):
    assert mounted_client.get("/v4/").status_code == 200
    errors = _unknown_errors(mounted_client.get("/v4/?bogus=1"))
    assert [entry["loc"] for entry in errors] == [["query", "bogus"]]


def test_v3_is_untouched(mounted_client):
    """The check lives on the v4 sub-app only; v3 stays frozen."""
    assert mounted_client.get("/v3/version?bogus=1").status_code != 422
