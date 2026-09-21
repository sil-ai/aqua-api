"""Tests for rate limiting on sensitive auth endpoints (issues #713/#950/#959).

Verifies that the slowapi limiter is wired into the endpoints that issue tokens, create
accounts and write passwords, and that exceeding a per-IP budget yields HTTP 429 (the
primary brute-force defense).

The two surfaces spell those endpoints differently, which is why the names are not
listed once: v3 has `/latest/token`, `/latest/users` and `/latest/change-password`,
while v4 has `/v4/token`, `/v4/users` and — since #950 split v3's single admin-only
`change-password` in two — both `/v4/users/me/password` and
`/v4/users/{user_id}/password`.

The v3 assertions can only check that a limit is *registered*: the admin dependency
runs before the limiter would reject an anonymous caller, and these tests predate any
admin token being available here. The v4 ones (#950) drive a real 429 instead, because
the shared ``admin_token`` fixture gets them past the gate.

The token endpoints are the exception to all of that, since #959: they hold no slowapi
decorator at all, because one would charge the budget before the handler ran and so
count the successful logins that ordinary service traffic is made of. They carry two
hand-spent failure budgets instead — a soft one charged after a 401, and a hard one
checked before any credential is evaluated — so their tests drive the endpoints and
assert on status codes rather than inspecting ``_route_limits``. There is one deliberate
exception, ``test_token_endpoints_carry_no_slowapi_decorator``, to catch a well-meaning
reinstatement of the decorator.

Two of these tests count password verifications rather than responses, because a status
code cannot tell you whether a guess was evaluated. A cap that answers 429 *after*
running bcrypt bounds nothing: the attacker still learns whether each guess was right.
``test_the_hard_gate_bounds_how_many_guesses_are_evaluated`` is the one that pins the
cap; the 429s elsewhere are a courtesy to well-behaved clients.
"""

import asyncio
import os
import subprocess
import sys

import pytest
from fastapi.testclient import TestClient
from limits import parse, parse_many
from slowapi.util import get_remote_address

from app import app
from database.models import UserDB
from security_routes import auth_routes
from security_routes.admin_routes import change_password, create_user
from security_routes.auth_routes import login_for_access_token
from security_routes.rate_limiting import (
    RATE_LIMIT_MESSAGE,
    TOKEN_FAILURE_RATE_LIMIT,
    TOKEN_HARD_FAILURE_LIMIT,
    TOKEN_HARD_FAILURE_RATE_LIMIT,
    TOKEN_HARD_LIMIT_SCOPE,
    TOKEN_LIMIT_SCOPE,
    TOKEN_SOFT_FAILURE_LIMIT,
    limiter,
)
from security_routes.utilities import NO_SUCH_USER_PASSWORD_HASH, verify_password
from security_routes.v4.group_routes import (
    add_group_member,
    create_group,
    delete_group,
    remove_group_member,
)
from security_routes.v4.token_routes import (
    login_for_access_token as v4_login_for_access_token,
)
from security_routes.v4.user_routes import change_own_password as v4_change_own_password
from security_routes.v4.user_routes import create_user as v4_create_user
from security_routes.v4.user_routes import reset_user_password as v4_reset_user_password

client = TestClient(app)
prefix = "/latest"


def _route_limit_key(func) -> str:
    return f"{func.__module__}.{func.__qualname__}"


def _override_route_limit(func, new_limit: str):
    """Replace the slowapi route limit on `func` with `new_limit`.

    Returns a `restore` callable that puts the original RateLimitItem back.

    Note: this reaches into ``limiter._route_limits``, which is a private
    slowapi attribute. We accept that brittleness because the alternative
    (a fresh Limiter + FastAPI app per test) is much heavier for a CI
    suite that shares a single module-scoped TestClient. The fixture
    always restores the original limit, so other tests in the same
    process are not affected. If slowapi changes this attribute name on
    a future upgrade, this helper will raise loudly via the
    ``AssertionError`` below — that's the signal to migrate the test.
    """
    func_name = _route_limit_key(func)
    limits = limiter._route_limits.get(func_name) or []
    if not limits:
        raise AssertionError(
            f"No slowapi route limit registered for {func_name}; "
            "did the @limiter.limit decorator get removed (or did "
            "slowapi rename _route_limits)?"
        )
    originals = [lim.limit for lim in limits]
    new_item = parse_many(new_limit)[0]
    for lim in limits:
        lim.limit = new_item
    limiter.reset()

    def restore():
        for lim, original in zip(limits, originals):
            lim.limit = original
        limiter.reset()

    return restore


GOOD = {"username": "testuser1", "password": "password1"}
BAD = {"username": "testuser1", "password": "wrongpassword"}
UNKNOWN = {"username": "no-such-user-9f2a", "password": "wrongpassword"}

SOFT = 3
HARD = 6


def _set_token_budgets(soft: str, hard: str):
    """Point both token budgets at small values and clear whatever they had counted.

    Returns a ``restore`` callable. ``limiter.reset()`` on the way in *and* out is
    what keeps these tests order-independent: the counters live in module-level
    storage shared by every test in the process, so any other module that fetched a
    token with bad credentials would otherwise leave them part-spent. One helper
    covers v3 and v4 because there is one pair of ``Limit`` objects between them —
    no decorator, so no per-route copy of the value to keep in step.
    """
    originals = (TOKEN_SOFT_FAILURE_LIMIT.limit, TOKEN_HARD_FAILURE_LIMIT.limit)
    TOKEN_SOFT_FAILURE_LIMIT.limit = parse_many(soft)[0]
    TOKEN_HARD_FAILURE_LIMIT.limit = parse_many(hard)[0]
    limiter.reset()

    def restore():
        TOKEN_SOFT_FAILURE_LIMIT.limit = originals[0]
        TOKEN_HARD_FAILURE_LIMIT.limit = originals[1]
        limiter.reset()

    return restore


@pytest.fixture
def tight_token_limits():
    """Both tiers small enough to exhaust: soft at 3 failures, hard at 6."""
    restore = _set_token_budgets(f"{SOFT}/minute", f"{HARD}/minute")
    try:
        yield
    finally:
        restore()


@pytest.fixture
def tight_soft_token_limit():
    """Only the soft tier is reachable; the hard gate stays open.

    This is the shape a real deployment is in almost all the time — 5 failures a
    minute is easy to hit by accident, 60 is not — so the tests about a
    *misconfigured* caller run against it rather than against the flood shape.
    """
    restore = _set_token_budgets(f"{SOFT}/minute", "10000/minute")
    try:
        yield
    finally:
        restore()


def _assert_sane_retry_after(response):
    """The 429 must say *when* to come back, with a number that means something.

    ``_retry_after_seconds`` swallows every exception and returns None, and it reads
    the ``view_rate_limit`` tuple these tiers now build by hand — so a header that is
    merely present proves nothing. The window is a minute, so anything outside 1..60
    is a bug in that computation rather than a plausible back-off.
    """
    header = response.headers.get("Retry-After")
    assert header is not None, dict(response.headers)
    assert 1 <= int(header) <= 60, header


def _count_password_checks(monkeypatch):
    """Record every bcrypt verification the token path actually performs.

    Returns the list it appends to. Status codes cannot distinguish a guess that was
    refused from one that was evaluated and merely answered 429 afterwards, and that
    distinction is the entire difference between a cap and a cosmetic error code.
    """
    checked = []
    real = auth_routes.verify_password

    def counting(plain_password, hashed_password):
        checked.append(hashed_password)
        return real(plain_password, hashed_password)

    monkeypatch.setattr(auth_routes, "verify_password", counting)
    return checked


def test_successful_logins_never_spend_either_budget(
    test_db_session, tight_token_limits
):
    """The regression #959 is about: a service that authenticates correctly.

    Ten in a row against budgets of three and six. Before the fix the limiter ran as
    a decorator, ahead of ``authenticate_user``, so the fourth of these was a 429 and
    ordinary service traffic locked itself out of the API. A healthy fleet fails
    approximately never, so neither tier should ever see it.
    """
    for _ in range(10):
        response = client.post(f"{prefix}/token", data=GOOD)
        assert response.status_code == 200, response.text


def test_v4_successful_logins_never_spend_either_budget(
    test_db_session, tight_token_limits
):
    """#959 again, on the surface new clients are being migrated to."""
    for _ in range(10):
        response = client.post("/v4/token", data=GOOD)
        assert response.status_code == 200, response.text


def test_one_bad_credential_is_a_401_not_a_429(test_db_session, tight_token_limits):
    """A single wrong password must still read as a wrong password."""
    response = client.post(f"{prefix}/token", data=BAD)
    assert response.status_code == 401, response.text


def test_the_soft_tier_answers_429_after_its_budget_of_failures(
    test_db_session, tight_soft_token_limit
):
    """Three failures are inside the soft budget and answer 401; the fourth is over.

    This tier does not stop anything on its own — it is charged after the credential
    has already been checked — but it is what turns a misconfigured client's retry
    loop into a clear back-off signal long before the hard gate would shut.
    """
    for _ in range(SOFT):
        response = client.post(f"{prefix}/token", data=BAD)
        assert response.status_code == 401, response.text

    response = client.post(f"{prefix}/token", data=BAD)
    assert response.status_code == 429, response.text


def test_a_spent_soft_budget_does_not_block_correct_credentials(
    test_db_session, tight_soft_token_limit
):
    """The graduated half of the design, and why the soft tier is worth having.

    Every Modal worker leaves from one address pool. A single container holding a
    stale password should cost that address its soft budget and nothing more, so the
    rest of the fleet keeps authenticating. The hard gate is what eventually refuses
    everyone — see ``test_the_hard_gate_refuses_correct_credentials_too`` — and this
    test is deliberately run with that gate left open.
    """
    for _ in range(SOFT + 2):
        client.post(f"{prefix}/token", data=BAD)

    assert client.post(f"{prefix}/token", data=BAD).status_code == 429

    response = client.post(f"{prefix}/token", data=GOOD)
    assert response.status_code == 200, response.text


def test_interleaved_successes_do_not_bring_the_429_forward(
    test_db_session, tight_soft_token_limit
):
    """Successes between failures must not shorten the runway.

    Three failures fit in the budget however many good logins are mixed in with
    them, which is what "only failures are counted" has to mean when both kinds of
    traffic arrive from one address at once.
    """
    for _ in range(SOFT):
        assert client.post(f"{prefix}/token", data=GOOD).status_code == 200
        assert client.post(f"{prefix}/token", data=BAD).status_code == 401

    assert client.post(f"{prefix}/token", data=GOOD).status_code == 200
    assert client.post(f"{prefix}/token", data=BAD).status_code == 429


def test_the_hard_gate_bounds_how_many_guesses_are_evaluated(
    test_db_session, tight_token_limits, monkeypatch
):
    """The cap. Guess *throughput* is bounded, not merely labelled 429.

    Eighty requests, a hard budget of six: exactly six credentials may be evaluated,
    and the count must not move when the attacker keeps going. Counting verifications
    rather than status codes is the point — charging a counter after
    ``authenticate_user`` has returned cannot stop the next guess, so a test that
    only asserts "something returned 429" passes just as happily against no cap at
    all.
    """
    checked = _count_password_checks(monkeypatch)

    codes = [client.post(f"{prefix}/token", data=BAD).status_code for _ in range(40)]

    assert len(checked) == HARD, f"{len(checked)} guesses evaluated, expected {HARD}"
    assert codes[:SOFT] == [401] * SOFT, codes
    assert set(codes[SOFT:]) == {429}, codes

    for _ in range(40):
        client.post(f"{prefix}/token", data=BAD)

    assert len(checked) == HARD, (
        f"{len(checked)} guesses evaluated after 80 requests; the gate is not "
        "bounding throughput, it is only labelling the responses"
    )


def test_the_v4_hard_gate_bounds_how_many_guesses_are_evaluated(
    test_db_session, tight_token_limits, monkeypatch
):
    """The same cap on v4, whose handler has to call the gate for itself."""
    checked = _count_password_checks(monkeypatch)

    codes = [client.post("/v4/token", data=BAD).status_code for _ in range(40)]

    assert len(checked) == HARD, f"{len(checked)} guesses evaluated, expected {HARD}"
    assert set(codes[SOFT:]) == {429}, codes


def test_the_two_tiers_do_not_share_a_counter(test_db_session, monkeypatch):
    """Each tier needs its own scope, and only an equal-budget run can show it.

    ``RateLimitItem`` builds its storage key out of the identifiers *and* the limit's
    own amount and granularity, so while the two tiers are configured with different
    numbers they land on different keys whatever their scopes say. Set them equal —
    which an operator tuning ``AUTH_TOKEN_HARD_FAILURE_LIMIT`` down could easily do —
    and a shared scope collapses them onto one counter that every failure charges
    twice, halving both the guess budget and the soft tier's runway. Four failures
    must be evaluated and answered 401, not two.
    """
    restore = _set_token_budgets("4/minute", "4/minute")
    try:
        checked = _count_password_checks(monkeypatch)
        codes = [client.post(f"{prefix}/token", data=BAD).status_code for _ in range(6)]
    finally:
        restore()

    assert len(checked) == 4, f"{len(checked)} guesses evaluated, expected 4"
    assert codes == [401, 401, 401, 401, 429, 429], codes


def test_the_hard_gate_refuses_correct_credentials_too(
    test_db_session, tight_token_limits, monkeypatch
):
    """The property #959's first fix gave away, asserted as a deliberate choice.

    A correct credential presented from an address that has spent the hard budget is
    refused, and no token is issued. It has to be: discovering that a credential is
    correct means evaluating it, so there is no way to honour every good login and
    also bound guessing. The cost is bounded in the other direction by the size of
    the hard budget, which a healthy caller never approaches because successes are
    not counted at all.
    """
    checked = _count_password_checks(monkeypatch)

    for _ in range(HARD + 1):
        client.post(f"{prefix}/token", data=BAD)

    response = client.post(f"{prefix}/token", data=GOOD)
    assert response.status_code == 429, response.text
    assert "access_token" not in response.json(), response.text
    assert len(checked) == HARD, "the correct credential was evaluated anyway"
    _assert_sane_retry_after(response)


def test_v3_and_v4_token_share_one_per_address_budget(
    test_db_session, tight_soft_token_limit
):
    """The two token endpoints must not each get their own budget.

    Scoping per decorated endpoint would let an attacker double their attempts per
    minute simply by alternating between ``/latest/token`` and ``/v4/token``. Both
    routes name the same scopes, so spending the whole soft budget on v4 must leave
    v3 already refusing.
    """
    for _ in range(SOFT + 1):
        client.post("/v4/token", data=BAD)

    response = client.post(f"{prefix}/token", data=BAD)
    assert response.status_code == 429, response.text


def test_v4_rate_limit_answers_in_the_v4_error_envelope(
    test_db_session, tight_soft_token_limit
):
    """v4's 429 must use v4's envelope, not v3's ``{"detail": ...}``.

    ``RateLimitExceeded`` is a ``StarletteHTTPException``, so v4's own handler shapes
    it and maps 429 to ``TOO_MANY_REQUESTS``. What that handler cannot supply is
    ``Retry-After`` — slowapi builds the exception with no headers at all — so this
    also pins the header v4 would otherwise silently drop while v3 kept it.
    """
    for _ in range(SOFT):
        client.post("/v4/token", data=BAD)

    response = client.post("/v4/token", data=BAD)
    assert response.status_code == 429, response.text
    body = response.json()
    assert "detail" not in body, body
    assert body["error"]["code"] == "TOO_MANY_REQUESTS", body
    _assert_sane_retry_after(response)

    # v4 renders ``exc.detail`` as the message, and ``RateLimitExceeded`` fills that
    # from ``str(limit.limit)`` when the Limit carries no ``error_message`` — which
    # published the configured budget, e.g. "3 per 1 minute", to anonymous callers.
    assert body["error"]["message"] == RATE_LIMIT_MESSAGE, body
    assert "per 1 minute" not in body["error"]["message"], body


def test_rate_limit_response_includes_retry_headers(
    test_db_session, tight_soft_token_limit
):
    """The 429 response should carry the standard ``Retry-After`` header
    so well-behaved clients know when to back off.

    Worth pinning again after #959: the header is computed from
    ``request.state.view_rate_limit``, which slowapi's decorator used to set and the
    hand-rolled tiers now have to set themselves.
    """
    for _ in range(SOFT):
        client.post(f"{prefix}/token", data=BAD)
    response = client.post(f"{prefix}/token", data=BAD)
    assert response.status_code == 429, response.text
    assert response.json().get("detail") == RATE_LIMIT_MESSAGE, response.text
    _assert_sane_retry_after(response)


def test_disabling_the_limiter_switches_off_both_token_tiers(
    test_db_session, tight_token_limits, monkeypatch
):
    """``RATELIMIT_ENABLED=false`` must silence the token path too.

    slowapi reads that through starlette's ``Config``, so an operator can set it
    from the environment with no code change and the ``@limiter`` decorators on
    ``/users`` and ``/change-password`` go quiet. A kill switch that silenced those
    but left ``/token`` returning 429s would be worse than no kill switch.

    Both budgets are filled *first*, with the limiter still on. Flipping the switch
    on empty buckets would prove nothing: neither tier had anything to refuse, so a
    version that ignored the flag entirely would look identical.
    """
    for _ in range(HARD + 1):
        client.post(f"{prefix}/token", data=BAD)
    assert client.post(f"{prefix}/token", data=BAD).status_code == 429

    monkeypatch.setattr(limiter, "enabled", False)

    for _ in range(HARD + 4):
        assert client.post(f"{prefix}/token", data=BAD).status_code == 401
    assert client.post(f"{prefix}/token", data=GOOD).status_code == 200


def test_an_unknown_username_still_costs_a_password_verification(
    test_db_session, tight_token_limits, monkeypatch
):
    """No timing oracle for username enumeration.

    ``not user or not verify_password(...)`` short-circuits, so a missing row used to
    skip bcrypt entirely and answer in ~18ms against ~175ms for a real one — one
    request classifies a username with certainty, which is exactly what the single
    shared 401 both surfaces return is designed to prevent. The fix is to verify
    against a fixed dummy hash instead, so this asserts the hash that was used.
    """
    checked = _count_password_checks(monkeypatch)

    response = client.post(f"{prefix}/token", data=UNKNOWN)
    assert response.status_code == 401, response.text
    assert checked == [NO_SUCH_USER_PASSWORD_HASH], checked


def test_the_dummy_hash_cannot_be_matched():
    """The checked-in hash must be a real bcrypt digest of nothing guessable.

    If it were ever replaced with a hash of a known string — or with something that
    is not a valid digest at all — every unknown username would authenticate, or
    every one would raise.
    """
    for guess in ("", "password", "password1", "wrongpassword"):
        assert not verify_password(guess, NO_SUCH_USER_PASSWORD_HASH)


def test_password_verification_runs_off_the_event_loop(
    test_db_session, tight_token_limits, monkeypatch
):
    """bcrypt must not be called from the coroutine.

    Cost 12 is ~154ms of uninterruptible CPU. Inline, that pins the event loop, so a
    burst of failed logins stalls every endpoint the worker serves rather than just
    this one. ``asyncio.get_running_loop()`` succeeds on the loop thread and raises
    off it, which is the difference being asserted.
    """
    ran_on_loop = []
    real = auth_routes.verify_password

    def probe(plain_password, hashed_password):
        try:
            asyncio.get_running_loop()
            ran_on_loop.append(True)
        except RuntimeError:
            ran_on_loop.append(False)
        return real(plain_password, hashed_password)

    monkeypatch.setattr(auth_routes, "verify_password", probe)

    assert client.post(f"{prefix}/token", data=GOOD).status_code == 200
    assert ran_on_loop == [False], "verify_password ran on the event loop thread"


def test_token_endpoints_carry_no_slowapi_decorator():
    """Neither token route may regain a ``@limiter`` decorator.

    A decorator runs before the handler, so re-adding one would silently restore the
    #959 behaviour — successful logins charged against a brute-force budget — while
    every other test here kept passing.
    """
    for func in (login_for_access_token, v4_login_for_access_token):
        key = _route_limit_key(func)
        assert not limiter._route_limits.get(key), (
            f"{key} has regained a slowapi decorator; it would charge successful "
            "logins against the brute-force budget again (#959)"
        )


def test_the_token_budgets_are_wired_to_the_env_vars_that_name_them(
    tight_token_limits,
):
    """Both tiers must be built from their own setting, with the intended key.

    Every behavioural test above replaces ``.limit`` with something small, so none of
    them would notice a budget hard-coded in the module, or one wired to
    ``USERS_RATE_LIMIT`` by a copy-paste. This reads the shipped objects instead.
    The fixture is here only to put the values back afterwards — the assertions are
    about the module constants, not about any counter.
    """
    assert TOKEN_SOFT_FAILURE_LIMIT.scope == TOKEN_LIMIT_SCOPE
    assert TOKEN_HARD_FAILURE_LIMIT.scope == TOKEN_HARD_LIMIT_SCOPE
    assert TOKEN_SOFT_FAILURE_LIMIT.key_func is get_remote_address
    assert TOKEN_HARD_FAILURE_LIMIT.key_func is get_remote_address

    restore = _set_token_budgets(
        TOKEN_FAILURE_RATE_LIMIT, TOKEN_HARD_FAILURE_RATE_LIMIT
    )
    try:
        assert TOKEN_SOFT_FAILURE_LIMIT.limit == parse(TOKEN_FAILURE_RATE_LIMIT)
        assert TOKEN_HARD_FAILURE_LIMIT.limit == parse(TOKEN_HARD_FAILURE_RATE_LIMIT)
    finally:
        restore()


AUTH_LIMIT_VARS = (
    "AUTH_TOKEN_FAILURE_LIMIT",
    "AUTH_TOKEN_HARD_FAILURE_LIMIT",
    "AUTH_USERS_RATE_LIMIT",
    "AUTH_CHANGE_PASSWORD_RATE_LIMIT",
)


def _token_budgets_in_a_fresh_process(overrides: dict) -> list:
    """Import the module in a subprocess under ``overrides`` and report both budgets.

    Every ``AUTH_*`` limit is dropped from the environment first, then ``overrides``
    is applied, so the subprocess sees exactly what is asked for and nothing the test
    runner happens to export. ``test/conftest.py`` relaxes all of them suite-wide
    before anything imports this module, which is why no in-process test can see what
    an operator actually gets. It prints ``Limit.limit``, not the string constant, so
    the answer covers the whole chain: variable name, default, ``parse``, and the
    object the handlers really spend.
    """
    env = {k: v for k, v in os.environ.items() if k not in AUTH_LIMIT_VARS}
    env.update(overrides)
    source = (
        "from security_routes import rate_limiting as r\n"
        "print(r.TOKEN_SOFT_FAILURE_LIMIT.limit, '|', r.TOKEN_HARD_FAILURE_LIMIT.limit)\n"
    )
    out = subprocess.run(
        [sys.executable, "-c", source],
        capture_output=True,
        text=True,
        check=True,
        env=env,
    )
    return [part.strip() for part in out.stdout.strip().split("|")]


def test_the_shipped_budget_defaults_are_five_and_sixty_a_minute():
    """What an operator who sets nothing actually gets — the brute-force ceiling.

    The one pair of numbers in this module worth pinning literally, because every
    behavioural test above replaces them with something small and would not notice a
    budget hard-coded at 500 a minute.
    """
    assert _token_budgets_in_a_fresh_process({}) == [
        "5 per 1 minute",
        "60 per 1 minute",
    ]


def test_each_token_tier_reads_its_own_setting():
    """Every auth budget is given a different value, so a crossed wire shows up.

    In the suite's own environment all four ``AUTH_*`` limits are relaxed to the same
    number, which means a tier accidentally wired to ``AUTH_USERS_RATE_LIMIT`` — the
    copy-paste this module invites, with four near-identical ``os.getenv`` lines —
    would look perfectly correct. Distinct values are what separate them.
    """
    assert _token_budgets_in_a_fresh_process(
        {
            "AUTH_TOKEN_FAILURE_LIMIT": "7/minute",
            "AUTH_TOKEN_HARD_FAILURE_LIMIT": "77/minute",
            "AUTH_USERS_RATE_LIMIT": "111/minute",
            "AUTH_CHANGE_PASSWORD_RATE_LIMIT": "222/minute",
        }
    ) == ["7 per 1 minute", "77 per 1 minute"]


def test_one_address_failures_do_not_lock_out_another(
    test_db_session, tight_soft_token_limit
):
    """The soft tier is keyed per address, not globally.

    Drop the key from the hit arguments and every caller on earth shares one bucket:
    five failed logins a minute would deny login to the whole platform. Nothing else
    here would notice, because ``TestClient`` gives every request the same peer
    (``testclient``), so the suite runs entirely inside one key.
    """
    attacker = TestClient(app, client=("203.0.113.7", 40000))
    bystander = TestClient(app, client=("198.51.100.9", 40000))

    for _ in range(SOFT + 1):
        attacker.post(f"{prefix}/token", data=BAD)
    assert attacker.post(f"{prefix}/token", data=BAD).status_code == 429

    assert bystander.post(f"{prefix}/token", data=BAD).status_code == 401
    assert bystander.post(f"{prefix}/token", data=GOOD).status_code == 200


def test_the_hard_gate_is_keyed_per_address_too(
    test_db_session, tight_token_limits, monkeypatch
):
    """And so is the pre-auth gate, which is the more dangerous of the two to key wrong.

    A global hard bucket would mean sixty failures anywhere shutting ``/token`` for
    everyone, correct credentials included. Asserted on evaluations rather than on
    the status code: the bystander's guess has to actually reach bcrypt, which is
    what proves the gate opened for them rather than the 401 arriving some other way.
    """
    attacker = TestClient(app, client=("203.0.113.7", 40000))
    bystander = TestClient(app, client=("198.51.100.9", 40000))

    for _ in range(HARD + 2):
        attacker.post(f"{prefix}/token", data=BAD)
    assert attacker.post(f"{prefix}/token", data=GOOD).status_code == 429

    checked = _count_password_checks(monkeypatch)
    assert bystander.post(f"{prefix}/token", data=BAD).status_code == 401
    assert len(checked) == 1, "the bystander never reached the password check"
    assert bystander.post(f"{prefix}/token", data=GOOD).status_code == 200


def test_users_endpoint_has_rate_limit_registered():
    """POST /users must have a slowapi route limit registered.

    The admin auth dependency runs before the limiter would reject anonymous
    callers, so we can't drive a 429 from an unauthenticated request here.
    We assert the limit decorator is present instead.
    """
    func_name = _route_limit_key(create_user)
    limits = limiter._route_limits.get(func_name) or []
    assert limits, f"Expected a slowapi route limit on {func_name}"


def test_change_password_endpoint_has_rate_limit_registered():
    """POST /change-password must have a slowapi route limit registered."""
    func_name = _route_limit_key(change_password)
    limits = limiter._route_limits.get(func_name) or []
    assert limits, f"Expected a slowapi route limit on {func_name}"


def test_v4_token_endpoint_is_rate_limited(test_db_session, tight_soft_token_limit):
    """v4's /token must be throttled too.

    It was added after #713 and shares ``authenticate_user`` with v3, so leaving it
    open would have moved brute-force one path to the left rather than closing it.
    """
    for _ in range(SOFT):
        response = client.post("/v4/token", data=BAD)
        assert response.status_code == 401, response.text

    response = client.post("/v4/token", data=BAD)
    assert response.status_code == 429, response.text


@pytest.fixture
def tight_v4_user_create_limit():
    """Tighten ``POST /v4/users`` to a budget a test can exhaust."""
    restore = _override_route_limit(v4_create_user, "3/minute")
    try:
        yield
    finally:
        restore()


@pytest.fixture
def tight_v4_password_limits():
    """Tighten *both* v4 password endpoints to the same small budget.

    ``shared_limit`` makes them draw on one counter, but each route still holds its
    own ``Limit`` object carrying the *value*, so overriding one would leave the
    other at the env default and the shared bucket would not fill at the rate the
    test expects.
    """
    restores = [
        _override_route_limit(v4_change_own_password, "3/minute"),
        _override_route_limit(v4_reset_user_password, "3/minute"),
    ]
    try:
        yield
    finally:
        for restore in restores:
            restore()


def _admin_auth(admin_token):
    return {"Authorization": f"Bearer {admin_token}"}


def test_v4_create_user_is_rate_limited(
    test_db_session, admin_token, tight_v4_user_create_limit
):
    """v3 throttles account creation at 5/minute; v4 must not ship it open.

    Every attempt here is a 409 on a username that already exists, which keeps the
    test from creating rows while still proving that *rejected* requests count
    against the budget — an attacker probing for taken usernames gets no free ride.
    """
    body = {"username": "testuser1", "password": "sentinel-swordfish-42"}
    for _ in range(3):
        response = client.post("/v4/users", json=body, headers=_admin_auth(admin_token))
        assert response.status_code == 409, response.text

    response = client.post("/v4/users", json=body, headers=_admin_auth(admin_token))
    assert response.status_code == 429, response.text
    assert response.json()["error"]["code"] == "TOO_MANY_REQUESTS"


def test_v4_password_writes_share_one_per_ip_budget(
    test_db_session, admin_token, tight_v4_password_limits
):
    """The two halves of v3's one ``POST /change-password`` must not get a budget each.

    #950 split it into a self-service ``POST /v4/users/me/password`` and an admin
    ``PUT /v4/users/{id}/password``. Without ``shared_limit`` that split would have
    doubled the per-IP budget for writing a password. Spend the whole budget on the
    self-service half with a wrong current password — which changes nothing — then
    assert the admin half is already refusing.
    """
    for _ in range(3):
        response = client.post(
            "/v4/users/me/password",
            json={
                "current_password": "not-the-current-one",
                "new_password": "sentinel-swordfish-42",
            },
            headers=_admin_auth(admin_token),
        )
        assert response.status_code == 403, response.text

    admin_id = (
        test_db_session.query(UserDB).filter(UserDB.username == "admin").first().id
    )
    response = client.put(
        f"/v4/users/{admin_id}/password",
        json={"new_password": "sentinel-swordfish-42"},
        headers=_admin_auth(admin_token),
    )
    assert response.status_code == 429, response.text


def test_v4_write_429_carries_retry_after_and_the_v4_envelope(
    test_db_session, admin_token, tight_v4_user_create_limit
):
    """The 429 on a write is shaped like every other v4 error, header included.

    Already pinned for ``/v4/token``; re-asserted on a *write* because that route is
    public and these are not, so they reach the limiter through a different
    dependency stack and could in principle be handled elsewhere.
    """
    body = {"username": "testuser1", "password": "sentinel-swordfish-42"}
    for _ in range(3):
        client.post("/v4/users", json=body, headers=_admin_auth(admin_token))

    response = client.post("/v4/users", json=body, headers=_admin_auth(admin_token))
    assert response.status_code == 429, response.text
    assert "detail" not in response.json(), response.text
    assert "retry-after" in {h.lower() for h in response.headers}


def test_v4_group_writes_are_not_rate_limited():
    """Stated as a decision rather than left to inference.

    v3 throttles account creation and password writes and nothing else, and the
    throttles exist for credential brute-forcing and unauthenticated signup. Group and
    membership writes are admin-only with nothing to guess, so they carry no limit —
    if that changes, this test is what has to change with it.
    """
    for func in (add_group_member, remove_group_member, create_group, delete_group):
        key = _route_limit_key(func)
        assert not limiter._route_limits.get(key), (
            f"{key} has acquired a rate limit; decide whether that is intended and "
            "update this test"
        )


def test_a_nul_byte_username_is_a_charged_401_not_an_uncaught_500(
    test_db_session, tight_soft_token_limit
):
    """A NUL byte must not escape as a 500 that charges neither budget.

    asyncpg raises CharacterNotInRepertoireError on a NUL in a query parameter
    (cf. #954). Before the guard in authenticate_user that surfaced as a 500
    raised ahead of register_failed_login, leaving an unauthenticated caller an
    unbounded source of DB round trips — the one bound the decorator used to
    provide for free.
    """
    nul = {"username": "admin\x00", "password": "whatever"}
    for _ in range(SOFT):
        assert client.post(f"{prefix}/token", data=nul).status_code == 401

    # And it spends the budget like any other failure, rather than bypassing it.
    assert client.post(f"{prefix}/token", data=nul).status_code == 429
