# test_jwt_migration.py
"""Regression tests for #938 — migrating JWT handling from python-jose to PyJWT.

The behavioural risk in that migration isn't cryptographic (HS256 is a
standard; any conformant library produces and accepts the same bytes) — it's
that the two libraries raise different exception types on a bad token. Every
``except`` on the auth path that used to turn a bad token into a 401 has to
keep doing that with PyJWT's exceptions, or a malformed/expired/tampered
token becomes an unhandled 500.

python-jose is fully removed from this repo's dependencies as part of this
migration (see pyproject.toml / uv.lock), so these tests cannot call into it
at run time. Instead:

* ``GOLDEN_VALID_TOKEN`` / ``GOLDEN_EXPIRED_TOKEN`` below are real HS256
  tokens minted with python-jose 3.4.0 *before* it was removed, using a
  standalone secret (unrelated to the app's ``SECRET_KEY``) so they are
  frozen, reproducible fixtures rather than something regenerated against a
  library that is no longer installed. This is the "old library signs, new
  library verifies" half of the compatibility claim, captured permanently.
* The reverse direction — a token minted by the app's PyJWT-based
  ``create_access_token`` verifies under any spec-compliant HS256 verifier,
  which is all python-jose's HS256 path ever was — is checked against a
  from-scratch, dependency-free HS256 verifier (stdlib ``hmac``/``hashlib``
  only), so proving it doesn't require re-adding python-jose (or any other
  JWT library) as a dependency just to make the point.
"""
import base64
import hashlib
import hmac
import json
from datetime import timedelta

import jwt
import pytest
from fastapi import status

from security_routes.auth_routes import create_access_token
from security_routes.utilities import ALGORITHM, SECRET_KEY

GOLDEN_SECRET = "golden-fixture-secret-do-not-use-in-prod"

# Minted with python-jose 3.4.0, immediately before it was removed from this
# repo:
#   from jose import jwt
#   jwt.encode(
#       {"sub": "golden_user", "is_admin": False, "exp": 4102444800},
#       "golden-fixture-secret-do-not-use-in-prod",
#       algorithm="HS256",
#   )
# exp=4102444800 is 2100-01-01T00:00:00Z — valid for the practical lifetime of
# this test.
GOLDEN_VALID_TOKEN = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJzdWIiOiJnb2xkZW5fdXNlciIsImlzX2FkbWluIjpmYWxzZSwiZXhwIjo0MTAyNDQ0ODAwfQ."
    "S0ZAa_MPqonK28xF8ORiADCDMLMWFByuzxnoR15N0hw"
)
# Same library, same secret and payload shape, but exp=1000000000
# (2001-09-09T01:46:40Z) — already expired.
GOLDEN_EXPIRED_TOKEN = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJzdWIiOiJnb2xkZW5fdXNlciIsImlzX2FkbWluIjpmYWxzZSwiZXhwIjoxMDAwMDAwMDAwfQ."
    "zZByZMlLYBdg7OxTEMtFhPHDQ6kTLaq19ufqvB3mOOU"
)


def _spec_hs256_verify(token: str, secret: str) -> dict:
    """A from-scratch, dependency-free HS256 JWT verifier (RFC 7519) — a
    stand-in for "any spec-compliant library, including python-jose",
    without reintroducing python-jose as a dependency purely to prove
    interop.
    """
    header_b64, payload_b64, sig_b64 = token.split(".")

    def _b64url_decode(segment: str) -> bytes:
        padding = "=" * (-len(segment) % 4)
        return base64.urlsafe_b64decode(segment + padding)

    header = json.loads(_b64url_decode(header_b64))
    assert header == {"alg": "HS256", "typ": "JWT"}

    signing_input = f"{header_b64}.{payload_b64}".encode()
    expected_sig = hmac.new(secret.encode(), signing_input, hashlib.sha256).digest()
    actual_sig = _b64url_decode(sig_b64)
    assert hmac.compare_digest(expected_sig, actual_sig), "signature mismatch"

    return json.loads(_b64url_decode(payload_b64))


class TestCrossLibraryCompatibility:
    """Sign with the old library, verify with the new, both directions —
    the property the whole migration rests on (#938)."""

    def test_pyjwt_decodes_a_token_minted_by_python_jose(self):
        payload = jwt.decode(GOLDEN_VALID_TOKEN, GOLDEN_SECRET, algorithms=[ALGORITHM])
        assert payload == {
            "sub": "golden_user",
            "is_admin": False,
            "exp": 4102444800,
        }

    def test_pyjwt_still_rejects_an_expired_jose_token(self):
        with pytest.raises(jwt.ExpiredSignatureError):
            jwt.decode(GOLDEN_EXPIRED_TOKEN, GOLDEN_SECRET, algorithms=[ALGORITHM])

    def test_a_pyjwt_minted_token_verifies_under_a_spec_compliant_hs256_verifier(
        self,
    ):
        """The reverse direction: create_access_token (now PyJWT) produces a
        token that any standard HS256 JWT verifier accepts — exactly the
        property python-jose's HS256 path relied on, which is why swapping
        libraries is safe for this app."""
        token = create_access_token(
            data={"sub": "new_user", "is_admin": True},
            expires_delta=timedelta(minutes=5),
        )
        payload = _spec_hs256_verify(token, SECRET_KEY)
        assert payload["sub"] == "new_user"
        assert payload["is_admin"] is True


class TestBadTokensReturn401NotAn500:
    """The specific regression #938 calls out: a missed exception-type
    mapping turns a bad token into an unhandled 500 instead of a 401.
    Exercised over HTTP, across both v3 and v4, and against both the
    regular-user (``auth_routes.get_current_user``) and admin
    (``admin_routes.get_current_admin``) dependencies."""

    @pytest.mark.parametrize(
        "path",
        ["/latest/users/me", "/v4/users/me"],
    )
    def test_garbage_token_is_401(self, client, path):
        resp = client.get(
            path, headers={"Authorization": "Bearer not-a-real-jwt-at-all"}
        )
        assert resp.status_code == status.HTTP_401_UNAUTHORIZED, resp.text

    @pytest.mark.parametrize(
        "path",
        ["/latest/users/me", "/v4/users/me"],
    )
    def test_expired_token_is_401(self, client, path):
        expired_token = create_access_token(
            data={"sub": "testuser1", "is_admin": False},
            expires_delta=timedelta(seconds=-1),
        )
        resp = client.get(path, headers={"Authorization": f"Bearer {expired_token}"})
        assert resp.status_code == status.HTTP_401_UNAUTHORIZED, resp.text

    @pytest.mark.parametrize(
        "path",
        ["/latest/users/me", "/v4/users/me"],
    )
    def test_tampered_signature_is_401(self, client, regular_token1, path):
        header_b64, payload_b64, sig_b64 = regular_token1.split(".")
        # Flip the signature so it no longer matches the header/payload, while
        # keeping it well-formed base64url — this exercises the "bad
        # signature" branch specifically, not just "unparseable garbage".
        tampered_sig = ("A" if sig_b64[0] != "A" else "B") + sig_b64[1:]
        tampered_token = f"{header_b64}.{payload_b64}.{tampered_sig}"

        resp = client.get(path, headers={"Authorization": f"Bearer {tampered_token}"})
        assert resp.status_code == status.HTTP_401_UNAUTHORIZED, resp.text

    def test_admin_route_rejects_garbage_token_with_401(self, client):
        """Same mapping in admin_routes.get_current_admin, which catches
        ``jwt.PyJWTError`` independently of auth_routes.get_current_user."""
        resp = client.get(
            "/latest/groups",
            headers={"Authorization": "Bearer not-a-real-jwt-at-all"},
        )
        assert resp.status_code == status.HTTP_401_UNAUTHORIZED, resp.text

    def test_admin_route_rejects_expired_token_with_401(self, client):
        expired_token = create_access_token(
            data={"sub": "admin", "is_admin": True},
            expires_delta=timedelta(seconds=-1),
        )
        resp = client.get(
            "/latest/groups",
            headers={"Authorization": f"Bearer {expired_token}"},
        )
        assert resp.status_code == status.HTTP_401_UNAUTHORIZED, resp.text
