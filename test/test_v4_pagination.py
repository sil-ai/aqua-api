"""Tests for the v4 pagination contract (issue #829, epic #842).

Pins the reusable pieces from :mod:`api_v4.pagination`:

* :class:`~api_v4.pagination.PaginationParams` applies the documented defaults
  (``limit=20``, ``offset=0``) and echoes caller-supplied values;
* :class:`~api_v4.pagination.V4Page` is exactly ``{items, total, limit, offset}``,
  with ``total`` independent of ``len(items)``;
* out-of-range inputs (``limit`` above ``MAX_LIMIT``, ``limit < 1``, ``offset < 0``)
  return **422 in the #828 ``VALIDATION_ERROR`` envelope** — the key integration
  point with the error contract; nothing new is invented here;
* the generic response model renders in the sub-app's OpenAPI schema.

A throwaway list route is attached to a freshly built /v4 sub-app (mirroring
test_v4_errors.py / test_v4_subapp.py). No DB is needed.
"""

import pytest
from fastapi import Depends
from fastapi.testclient import TestClient

from api_v4.app import create_v4_app
from api_v4.pagination import DEFAULT_LIMIT, MAX_LIMIT, PaginationParams, V4Page
from api_v4.schemas.base import V4BaseModel

# next_updated_since is part of the one shared envelope (#899): it carries the
# delta watermark on lists that support updated_since and is null on the rest,
# rather than a delta response having a different shape from a full one.
ENVELOPE_KEYS = {"items", "total", "limit", "offset", "next_updated_since"}

# A distinctive total that can never equal len(items) below, so a test that the
# envelope's `total` is the full-result count (not the page length) can't pass by
# coincidence.
TOTAL_ROWS = 128


class WidgetOut(V4BaseModel):
    """A minimal item model, only so the generic ``V4Page[WidgetOut]`` has a
    concrete type argument to render in OpenAPI."""

    id: int
    name: str


PAGE_ITEMS = [WidgetOut(id=1, name="alpha"), WidgetOut(id=2, name="beta")]


@pytest.fixture
def client():
    """A /v4 sub-app with a throwaway list route using the pagination contract.

    CORS is a no-op so assertions isolate pagination/validation from the CORS
    layer (its own concern, covered in test_v4_subapp.py).
    """
    v4_app = create_v4_app(configure_cors=lambda _app: None)

    @v4_app.get("/_widgets", response_model=V4Page[WidgetOut])
    async def _list_widgets(page: PaginationParams = Depends()):
        # A real endpoint would slice its query by page.limit/page.offset and count
        # matching rows for `total`; here we return a fixed slice and a fixed total
        # so the echoed limit/offset and the total/items round-trip are observable.
        return V4Page.create(items=PAGE_ITEMS, total=TOTAL_ROWS, pagination=page)

    with TestClient(v4_app) as c:
        yield c


@pytest.fixture
def openapi(client):
    """The sub-app's generated OpenAPI document, built once for the OpenAPI tests
    (generation walks every route/schema, so avoid regenerating per assertion)."""
    return client.app.openapi()


def _assert_validation_envelope(response):
    """The response is a 422 shaped by the #828 error contract:
    ``{"error": {"code": "VALIDATION_ERROR", ...}}``."""
    assert response.status_code == 422
    body = response.json()
    assert set(body) == {"error"}, f"expected the #828 envelope, got {set(body)}"
    assert body["error"]["code"] == "VALIDATION_ERROR"
    # The offending query param is reported under details.errors (jsonable).
    errors = body["error"]["details"]["errors"]
    assert isinstance(errors, list) and errors


def test_defaults_applied_when_no_query_params(client):
    response = client.get("/_widgets")
    assert response.status_code == 200
    body = response.json()
    assert body["limit"] == DEFAULT_LIMIT == 20
    assert body["offset"] == 0


def test_envelope_has_exactly_the_contract_keys(client):
    body = client.get("/_widgets").json()
    assert set(body) == ENVELOPE_KEYS, f"unexpected envelope keys: {set(body)}"


def test_total_and_items_echo_correctly(client):
    body = client.get("/_widgets").json()
    # `total` is the full-result count, deliberately != the number of items on the
    # page — the envelope must report both independently.
    assert body["total"] == TOTAL_ROWS
    assert len(body["items"]) == len(PAGE_ITEMS) < body["total"]
    assert body["items"] == [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]


def test_supplied_limit_and_offset_are_echoed(client):
    body = client.get("/_widgets", params={"limit": 50, "offset": 10}).json()
    assert body["limit"] == 50
    assert body["offset"] == 10


def test_boundary_values_are_accepted(client):
    # The inclusive bounds themselves are valid: limit=1, limit=MAX_LIMIT, offset=0.
    assert client.get("/_widgets", params={"limit": 1}).status_code == 200
    at_max = client.get("/_widgets", params={"limit": MAX_LIMIT})
    assert at_max.status_code == 200
    assert at_max.json()["limit"] == MAX_LIMIT == 100
    assert client.get("/_widgets", params={"offset": 0}).status_code == 200


def test_limit_above_max_rejected_with_422_envelope(client):
    _assert_validation_envelope(
        client.get("/_widgets", params={"limit": MAX_LIMIT + 1})
    )


def test_limit_below_one_rejected_with_422_envelope(client):
    _assert_validation_envelope(client.get("/_widgets", params={"limit": 0}))


def test_negative_offset_rejected_with_422_envelope(client):
    _assert_validation_envelope(client.get("/_widgets", params={"offset": -1}))


def test_non_numeric_limit_rejected_with_422_envelope(client):
    # A non-integer value hits Pydantic type coercion (int_parsing) rather than a
    # range check, but must still surface as the same #828 VALIDATION_ERROR envelope.
    _assert_validation_envelope(client.get("/_widgets", params={"limit": "abc"}))


def test_openapi_contains_paginated_response_schema(openapi):
    # The sub-app's own OpenAPI must contain the generic page schema and reference
    # it from the list route's 200 response. create_v4_app now also mounts real
    # paginated resource endpoints (e.g. Versions -> V4Page_VersionOut_), so there
    # can be several V4Page_* schemas; select the throwaway widget route's own by
    # its item type rather than assuming it is the only one.
    page_schemas = [
        name for name in openapi["components"]["schemas"] if name.startswith("V4Page")
    ]
    widget_pages = [name for name in page_schemas if "WidgetOut" in name]
    # Exactly one paginated schema for THIS route's item type — asserted
    # semantically rather than pinning the exact generated identifier, whose
    # format can shift across FastAPI/Pydantic versions.
    assert len(widget_pages) == 1, page_schemas
    widget_page = widget_pages[0]

    ok_content = openapi["paths"]["/_widgets"]["get"]["responses"]["200"]["content"]
    ref = ok_content["application/json"]["schema"]["$ref"]
    assert ref == f"#/components/schemas/{widget_page}", ref

    # The envelope's numeric fields carry their non-negativity constraints into
    # the schema (self-documenting responses): total/offset >= 0, limit >= 1.
    props = openapi["components"]["schemas"][widget_page]["properties"]
    assert props["total"]["minimum"] == 0
    assert props["offset"]["minimum"] == 0
    assert props["limit"]["minimum"] == 1


def test_openapi_declares_limit_offset_query_params(openapi):
    # The other half of the contract: limit/offset must render as documented query
    # params (bounds + defaults) on the list route, so every list endpoint that
    # depends on PaginationParams advertises pagination identically in OpenAPI.
    params = {p["name"]: p for p in openapi["paths"]["/_widgets"]["get"]["parameters"]}
    assert set(params) == {"limit", "offset"}

    limit = params["limit"]
    assert limit["in"] == "query" and limit["required"] is False
    assert limit["schema"]["default"] == DEFAULT_LIMIT == 20
    assert limit["schema"]["minimum"] == 1
    assert limit["schema"]["maximum"] == MAX_LIMIT == 100

    offset = params["offset"]
    assert offset["in"] == "query" and offset["required"] is False
    assert offset["schema"]["default"] == 0
    assert offset["schema"]["minimum"] == 0
    assert "maximum" not in offset["schema"]
