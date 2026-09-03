"""What ``/v4/openapi.json`` publishes about the v4 API (issue #928, epic #842).

v4's audience is entirely new clients, the migration guide tells them the schema is the
authority on what an environment serves, and ``/v4/docs`` is publicly reachable — so the
schema is a wire contract, and these tests treat it as one. Three defects motivated the
module, all of them cases where the runtime was right and the published document was
wrong: every operation documented FastAPI's default ``HTTPValidationError`` instead of
the #828 error envelope, ``securitySchemes.tokenUrl`` named ``/v4/latest/token`` (a 404,
inherited from frozen v3) instead of ``/v4/token``, and the job headers were undeclared.

**Everything here asserts against the generated schema, never the source.** A test that
reads ``responses=`` back out of a decorator proves only that the decorator says what it
says; the artifact clients consume is what FastAPI generates from it, and the two are
not the same thing (FastAPI injects a ``422``, merges router-level and route-level
``responses``, and fills a response's ``content`` from ``response_model``).

The ``schema`` fixture fetches the schema over HTTP from the *mounted* app rather than
calling ``create_v4_app(...).openapi()``, because ``servers: [{"url": "/v4"}]`` comes
from the mount's ``root_path`` and is absent when the sub-app is built standalone — and
``servers`` is exactly what makes the relative ``tokenUrl`` resolvable. No database is
needed: schema generation never opens a connection.

One asymmetry worth stating, since it looks like an omission: this module does not
re-assert that the *runtime* sets the job headers. That is already pinned in
``test/test_assessment_routes/test_assessment_routes_v4.py`` (``Location`` and
``Retry-After`` on the 202 submit, ``Retry-After`` on a ``PENDING`` 202 and a
``RUNNING`` 200, and its absence on a terminal 200). This module pins the *documented*
half of the same facts. FastAPI never checks one against the other, so both halves need
their own test.
"""

import json

import fastapi
import pytest
from fastapi.testclient import TestClient

import app as app_module
from api_v4.errors import V4_ERROR_RESPONSE_REF

#: What every documented v4 error must point at.
V4_ERROR_REF = "#/components/schemas/V4ErrorResponse"

#: The statuses ``V4_ERROR_RESPONSES`` puts on every authenticated domain operation.
DOMAIN_ERROR_STATUSES = frozenset({"401", "403", "404", "422", "500"})


@pytest.fixture(scope="module")
def schema():
    """The v4 schema as a client receives it — fetched through the ``/v4`` mount.

    Module-scoped: it is an immutable document and building the app is the slow part.
    """
    mock_app = fastapi.FastAPI()
    app_module.configure(mock_app)
    with TestClient(mock_app) as client:
        response = client.get("/v4/openapi.json")
    assert response.status_code == 200, response.text
    return response.json()


def _operations(schema):
    """Every ``(path, method, operation)`` in the schema."""
    return [
        (path, method, operation)
        for path, item in schema["paths"].items()
        for method, operation in item.items()
    ]


class TestPublishedErrorContract:
    """Part 1: the documented error body is the one v4 actually sends."""

    def test_every_protected_operation_declares_the_v4_error_envelope(self, schema):
        """Asserted over *every* authenticated operation, not a sample.

        A representative check would pass while a router registered without
        ``responses=`` went undocumented, which is the failure this guards.
        """
        checked = 0
        for path, method, operation in _operations(schema):
            if "security" not in operation:
                continue
            checked += 1
            missing = DOMAIN_ERROR_STATUSES - set(operation["responses"])
            assert not missing, f"{method.upper()} {path} does not declare {missing}"
            for code in DOMAIN_ERROR_STATUSES:
                ref = operation["responses"][code]["content"]["application/json"][
                    "schema"
                ]["$ref"]
                assert ref == V4_ERROR_REF, f"{method.upper()} {path} {code} -> {ref}"
        assert checked > 1, "no protected operations found — fixture or wiring is wrong"

    def test_validation_errors_no_longer_document_the_fastapi_default(self, schema):
        """422 was the one error v4 *did* document, and it documented the wrong body.

        ``HTTPValidationError`` describes ``{"detail": [...]}``; v4 sends
        ``{"error": {"code": "VALIDATION_ERROR", ...}}``. Declaring our own 422 is what
        suppresses FastAPI's injected default, so its total absence from the document
        is the check — a leftover would mean some route slipped past the shared set.
        """
        assert "HTTPValidationError" not in schema["components"]["schemas"]
        assert "HTTPValidationError" not in json.dumps(schema)

    @pytest.mark.parametrize(
        ("path", "method", "code"),
        [
            ("/versions", "post", "400"),
            ("/versions/{version_id}", "patch", "400"),
            ("/revisions", "post", "400"),
            ("/revisions/{revision_id}", "patch", "400"),
            ("/assessments", "post", "409"),
            ("/assessments", "post", "503"),
        ],
    )
    def test_per_route_statuses_are_declared(self, schema, path, method, code):
        """The statuses only *some* operations answer, declared on those operations.

        These are the five operations that raise beyond the shared floor — found by
        walking the AST for ``status.HTTP_*`` in each handler and its helpers, not by
        reading for them. Leaving them out would have reproduced this PR's own defect:
        ``create_assessment``'s docstring promises a 409 and a 503, and the schema said
        neither.
        """
        response = schema["paths"][path][method]["responses"][code]
        assert response["content"]["application/json"]["schema"]["$ref"] == V4_ERROR_REF

    @pytest.mark.parametrize(
        ("path", "method"),
        [("/versions", "get"), ("/assessments/{assessment_id}", "get")],
    )
    def test_per_route_statuses_stay_off_operations_that_cannot_raise_them(
        self, schema, path, method
    ):
        """The other half of the point: 400/409/503 are *not* in the shared set.

        Had they been added there, they would have landed on all 33 operations — which
        is the over-documentation this PR already accepts for the five-status floor and
        should not extend any further.
        """
        declared = set(schema["paths"][path][method]["responses"])
        assert declared & {"400", "409", "503"} == set()

    def test_the_error_envelope_ref_resolves(self, schema):
        """``V4_ERROR_RESPONSE_REF`` is a hand-built string, so pin it to reality.

        :func:`api_v4.errors.json_error_responses` cannot pass ``model=`` (that is what
        forces the wrong media type), so it names the schema by ``$ref`` instead. A
        dangling ``$ref`` renders as an empty body in tooling and raises nothing here,
        which is exactly the kind of failure a test has to supply.
        """
        # Checked against this module's own literal, not re-derived from the class, so
        # renaming V4ErrorResponse cannot quietly move both sides together.
        assert V4_ERROR_RESPONSE_REF == V4_ERROR_REF
        prefix = "#/components/schemas/"
        assert V4_ERROR_RESPONSE_REF[len(prefix) :] in schema["components"]["schemas"]

    def test_a_plaintext_route_documents_json_errors(self, schema):
        """``GET /v4/revisions/{id}/text`` returns plaintext but errors in JSON.

        FastAPI documents a ``model=`` response at the *route's* media type, so the
        shared error set advertised five ``text/plain`` error bodies here — the one
        place on the v4 surface where the success type and the error type differ. The
        error handlers return a ``JSONResponse`` regardless.
        """
        responses = schema["paths"]["/revisions/{revision_id}/text"]["get"]["responses"]
        assert set(responses["200"]["content"]) == {"text/plain"}
        for code in DOMAIN_ERROR_STATUSES:
            assert set(responses[code]["content"]) == {"application/json"}, code

    def test_the_discovery_root_documents_no_authentication_errors(self, schema):
        """``GET /v4/`` is public and takes no input: only its 200 and a 500 apply.

        Declaring a 401 on an unauthenticated route, or a 422 on one with nothing to
        validate, would document errors it cannot return.
        """
        assert set(schema["paths"]["/"]["get"]["responses"]) == {"200", "500"}

    def test_the_token_endpoint_documents_its_own_401(self, schema):
        """``POST /v4/token`` answers 401, but for bad credentials, not a bad token.

        It is declared on the route so it can say ``INVALID_CREDENTIALS`` rather than
        inheriting the protected-route wording about a missing bearer token.
        """
        responses = schema["paths"]["/token"]["post"]["responses"]
        assert set(responses) == {"200", "401", "422", "500"}
        assert (
            responses["401"]["content"]["application/json"]["schema"]["$ref"]
            == V4_ERROR_REF
        )
        assert "INVALID_CREDENTIALS" in responses["401"]["description"]
        # No 403: nothing here can be forbidden, because nothing is authenticated.
        assert "403" not in responses
