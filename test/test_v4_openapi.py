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
from fastapi.dependencies.utils import get_flat_dependant
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

import app as app_module
from api_v4.app import create_v4_app
from api_v4.errors import V4_ERROR_RESPONSE_REF
from api_v4.jobs import JobState
from security_routes.auth_routes import oauth2_scheme as v3_oauth2_scheme
from security_routes.v4.dependencies import v4_oauth2_scheme

#: What every documented v4 error must point at.
V4_ERROR_REF = "#/components/schemas/V4ErrorResponse"

#: The statuses ``V4_ERROR_RESPONSES`` puts on every authenticated domain operation.
#: 403 is deliberately absent — see :class:`TestForbiddenIsWriteOnly`.
DOMAIN_ERROR_STATUSES = frozenset({"401", "404", "422", "500"})

#: The nine operations that can actually answer 403, enumerated by walking each
#: handler and its helpers for ``status.HTTP_403_FORBIDDEN`` (and for a ``require_admin``
#: dependency). Written out rather than computed so the test states the expected
#: surface instead of re-deriving whatever the code currently does.
FORBIDDEN_OPERATIONS = frozenset(
    {
        ("post", "/versions"),
        ("patch", "/versions/{version_id}"),
        ("put", "/versions/{version_id}/groups/{group_id}"),
        ("delete", "/versions/{version_id}/groups/{group_id}"),
        ("delete", "/versions/{version_id}"),
        ("patch", "/revisions/{revision_id}"),
        ("delete", "/revisions/{revision_id}"),
        ("delete", "/assessments/{assessment_id}"),
        ("get", "/groups"),
    }
)

ASSESSMENTS_PATH = "/assessments"
POLL_PATH = "/assessments/{assessment_id}"


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


@pytest.fixture(scope="module")
def v4_app():
    """The sub-app itself, for the assertions that inspect wiring rather than output."""
    return create_v4_app(configure_cors=app_module.configure_cors)


def _operations(schema):
    """Every ``(path, method, operation)`` in the schema."""
    return [
        (path, method, operation)
        for path, item in schema["paths"].items()
        for method, operation in item.items()
    ]


def _security_schemes_in_use(v4_app):
    """The ``SecurityBase`` *objects* FastAPI would harvest from the v4 route tree.

    This is the same traversal ``fastapi.openapi.utils.get_openapi`` performs, so it
    sees precisely what the schema would be built from — but it yields the instances
    rather than the rendered dict, which is what makes a v3 leak detectable at all.
    See :func:`test_no_v4_route_depends_on_the_v3_security_scheme`.
    """
    schemes = set()
    for route in v4_app.routes:
        if not isinstance(route, APIRoute):
            continue
        for requirement in get_flat_dependant(route.dependant).security_requirements:
            schemes.add(requirement.security_scheme)
    return schemes


class TestSecurityScheme:
    """Part 2: ``securitySchemes`` names an endpoint that exists."""

    def test_exactly_one_security_scheme_is_published(self, schema):
        schemes = schema["components"]["securitySchemes"]
        assert set(schemes) == {"OAuth2PasswordBearer"}, (
            "v4 should publish exactly one security scheme. A second one means a route "
            "pulled in a differently-named SecurityBase dependency."
        )

    def test_token_url_resolves_to_the_working_token_endpoint(self, schema):
        """The whole point of #928: the published ``tokenUrl`` must not 404.

        ``tokenUrl`` is relative, so it only means anything against ``servers`` — the
        pair is what the ``/v4/docs`` Authorize button and any generated OAuth2 client
        resolve. It read ``latest/token`` before this fix, i.e. ``/v4/latest/token``.
        """
        flow = schema["components"]["securitySchemes"]["OAuth2PasswordBearer"]["flows"][
            "password"
        ]
        base = schema["servers"][0]["url"]
        assert base == "/v4", "the mount's root_path is what makes tokenUrl resolvable"
        assert f"{base}/{flow['tokenUrl']}" == "/v4/token"

        # ...and that resolved path is a real POST on this very schema, not just a
        # plausible-looking string.
        assert "post" in schema["paths"]["/token"]

    def test_no_v4_route_depends_on_the_v3_security_scheme(self, v4_app):
        """The leak detector — and the reason it is not a scheme *count* assertion.

        FastAPI keys ``securitySchemes`` by ``scheme_name``, which defaults to the
        class name. The v3 and v4 schemes are both ``OAuth2PasswordBearer``, so wiring
        the v3 dependency back into a v4 route does **not** add a second entry: the two
        collide on one key and the last route processed wins, silently restoring the
        broken ``tokenUrl`` while the count still reads 1. ``group_router`` is
        registered last in ``api_v4.app``, which makes it the most dangerous place for
        one to reappear.

        Comparing scheme *objects* has no such blind spot.
        """
        in_use = _security_schemes_in_use(v4_app)
        assert in_use == {v4_oauth2_scheme}, (
            "every v4 route must authenticate through get_current_user_v4; a v3 "
            "get_current_user dependency puts auth_routes' scheme back in the tree"
        )
        assert v3_oauth2_scheme not in in_use

    def test_the_v3_scheme_is_untouched(self):
        """v3 is frozen: this fix added a v4 scheme, it did not edit v3's.

        ``latest/token`` is *correct* for v3, which is mounted at ``/latest``. Pinned
        here so a future "tidy-up" that redirects the v3 scheme at v4's endpoint fails
        loudly rather than breaking every v3 client's Authorize button.
        """
        assert v3_oauth2_scheme.model.flows.password.tokenUrl == "latest/token"


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


class TestForbiddenIsWriteOnly:
    """403 is declared on the nine operations that can raise it, and nowhere else.

    v4 answers ``404`` for a resource the caller may not see — so that ids cannot be
    probed — which leaves ``403`` meaning only "visible, but not yours". That makes it a
    write-path status: reachable on 9 of the 31 domain operations, unreachable on 22.

    It briefly *was* in the shared set, which published it on all 31. This class is what
    keeps it out: a client generated from the schema would otherwise carry dead
    forbidden-handling on every read, and a reader of ``/v4/docs`` would conclude any
    v4 call can be refused.
    """

    def test_exactly_the_write_paths_declare_403(self, schema):
        declared = {
            (method, path)
            for path, method, operation in _operations(schema)
            if "security" in operation and "403" in operation["responses"]
        }
        assert declared == FORBIDDEN_OPERATIONS, (
            "unexpected: "
            f"{sorted(declared - FORBIDDEN_OPERATIONS)}; missing: "
            f"{sorted(FORBIDDEN_OPERATIONS - declared)}"
        )

    def test_the_declared_403s_use_the_error_envelope(self, schema):
        for method, path in sorted(FORBIDDEN_OPERATIONS):
            response = schema["paths"][path][method]["responses"]["403"]
            ref = response["content"]["application/json"]["schema"]["$ref"]
            assert ref == V4_ERROR_REF, f"{method.upper()} {path}"

    def test_no_read_of_a_collection_claims_403(self, schema):
        """The clearest cases, spelled out: a plain list cannot be forbidden."""
        for path in ("/versions", "/revisions", "/assessments", "/users/me"):
            assert "403" not in schema["paths"][path]["get"]["responses"], path


class TestJobHeaders:
    """Part 3: ``Location`` and ``Retry-After`` are discoverable.

    The asymmetry between submit and poll is the contract, so each test pins one side
    of it *including* what the other side must not claim.
    """

    def test_submit_declares_both_job_headers(self, schema):
        headers = schema["paths"][ASSESSMENTS_PATH]["post"]["responses"]["202"][
            "headers"
        ]
        assert set(headers) == {"Location", "Retry-After"}
        # Both are unconditional on a 202: job_accepted_response refuses to build one
        # without a poll_url or with a sub-1 cadence.
        assert headers["Location"]["required"] is True
        assert headers["Retry-After"]["required"] is True

    def test_the_poll_never_declares_a_location(self, schema):
        """A poll answers *at* the poll URL; ``set_poll_headers`` sets no ``Location``.

        Declaring one would send a generated client looking for a header that is never
        there.
        """
        responses = schema["paths"][POLL_PATH]["get"]["responses"]
        for code in ("200", "202"):
            assert "Location" not in responses[code].get("headers", {}), code

    def test_the_poll_202_requires_retry_after(self, schema):
        """A 202 means ``PENDING``, which is never terminal, so the header is certain."""
        assert not JobState.PENDING.is_terminal
        headers = schema["paths"][POLL_PATH]["get"]["responses"]["202"]["headers"]
        assert set(headers) == {"Retry-After"}
        assert headers["Retry-After"]["required"] is True

    def test_the_poll_200_declares_retry_after_as_optional(self, schema):
        """The 200 is the response a polling loop sees most, and it is the subtle one.

        ``RUNNING`` is non-terminal but answers 200, so the cadence hint rides on the
        200 as well — leaving it undeclared would hide the header on the *common* poll.
        It cannot be ``required``, because the terminal states share that same 200 and
        deliberately carry no ``Retry-After``.
        """
        assert not JobState.RUNNING.is_terminal
        assert JobState.SUCCEEDED.is_terminal and JobState.FAILED.is_terminal

        response = schema["paths"][POLL_PATH]["get"]["responses"]["200"]
        assert set(response["headers"]) == {"Retry-After"}
        assert response["headers"]["Retry-After"]["required"] is False
        # Declaring a bare `headers` entry for the 200 must not cost it the body
        # schema FastAPI generates from `response_model`.
        assert "content" in response
