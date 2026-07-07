# Copilot Instructions — AQuA API

AQuA (Augmented Quality Assessment) API is an async **FastAPI** service for
Bible-translation quality assessment. It manages bible versions, revisions, and
verses; runs assessments (semantic-similarity, word-alignment, ngrams, tfidf,
text-lengths, agent-critique, etc.); and stores agent artifacts (word
alignments, lexeme cards, critiques, pivots). Persistence is **PostgreSQL** with
**pgvector** via **async SQLAlchemy 1.4 / asyncpg**, schema is managed with
**alembic**, and heavy compute is dispatched to **Modal** serverless functions
(`modal.Function.from_name(...).spawn.aio(...)`). The HTTP surface is versioned
as **v1 / v2 / v3** (plus `/latest`), with auth via **OAuth2 / JWT**.

When reviewing pull requests, focus on **endpoint integrity**, **consistency
with the existing endpoints**, and **sound design / SOLID principles**. Use the
guidance below.

## 1. Endpoint integrity

Every new or modified endpoint must be complete and safe end to end:

- Lives in the correct `<feature>_routes/v3/<name>_routes.py` module (mirroring
  `assessment_routes/`, `bible_routes/`, `agent_routes/`, etc.), exposed via a
  module-level `router = fastapi.APIRouter()`, and wired into `app.py` (imported
  and registered with `app.include_router(...)` under both `/v3` and `/latest`).
- Is declared `async def` and never performs blocking I/O on the event loop. All
  DB access goes through the injected `AsyncSession` (`await db.execute(...)`,
  `await db.commit()`, `await db.refresh(...)`); long/CPU-bound work is dispatched
  to Modal via `spawn.aio(...)` rather than run inline.
- Authenticates the caller with `current_user: UserModel = Depends(get_current_user)`
  (from `security_routes.auth_routes`) and gets its DB session via
  `db: AsyncSession = Depends(get_db)` (from `database.dependencies`).
- Enforces ownership / access on every read and write. Non-admin callers must be
  scoped to the bible versions they can reach through their groups
  (`UserGroup` → `BibleVersionAccess`); reuse the helpers in
  `security_routes/utilities.py` (`is_user_authorized_for_bible_version`,
  `is_user_authorized_for_revision`, `is_user_authorized_for_assessment`,
  `get_authorized_revision_ids`). `current_user.is_admin` is the established
  bypass; do not invent new ad-hoc auth logic.
- Validates all inputs explicitly. Reject bad input early with
  `HTTPException(status_code=status.HTTP_400_BAD_REQUEST, ...)` (or `422` for
  semantic-but-unprocessable bodies); return `404` for missing rows, `403` for
  authorization failures, `409` for conflicts/duplicates. Validate referenced
  FKs (e.g. `revision_id`, `version_id`) before committing so an unknown ID
  surfaces as a clean `404` instead of a `500` from an `IntegrityError`.
- Wraps DB work in `try/except SQLAlchemyError` (and `IntegrityError` where a
  unique constraint can be hit), calls `await db.rollback()` on failure, logs via
  the module `logger`, and raises `HTTPException` with the appropriate
  `status.HTTP_*` constant — never let unhandled exceptions leak stack traces and
  never return ad-hoc error dicts. Re-raise caught `HTTPException` unchanged
  (`except HTTPException: raise`) so intended status codes aren't swallowed.
- Has a clear docstring describing inputs, behavior, and the return value,
  consistent with sibling endpoints (which document fields and status codes).

## 2. Consistency with existing endpoints (same patterns)

New endpoints must match the established conventions — flag deviations:

- **API versioning:** new work lands in `v3` (`<feature>_routes/v3/...`). Each
  module starts with `__version__ = "v3"`. The same v3 router is registered under
  both `/v3` and `/latest` in `app.py`; v1/v2 are frozen legacy (imported
  conditionally behind `OMIT_PREVIOUS_VERSIONS`) — do not add features to them.
- **Router registration:** after adding a router, import it in `app.py` and add
  matching `include_router(..., prefix="/v3", tags=["Version 3"])` and
  `include_router(..., prefix="/latest", tags=["Version 3 / Latest"])` lines.
  A new module with no registration is dead code.
- **Naming:** route paths are kebab/word style under the feature
  (`/agent/word-alignment`, `/assessment`, `/version`); routers are named
  `router` and imported as `<feature>_router_v3`. Response models follow the
  `XxxIn` / `XxxOut` (and version-suffixed `XxxOut_v3`) convention.
- **Pydantic contracts:** request/response bodies are Pydantic models defined in
  the top-level `models.py`; SQLAlchemy ORM models live in `database/models.py`.
  Declare `response_model=` on the route and build the response with
  `XxxOut.model_validate(orm_obj)` rather than returning raw ORM objects or
  hand-built dicts. Keep `models.py` and `database/models.py` separate — don't
  return ORM rows directly where a Pydantic `*Out` exists.
- **Async-job pattern (assessments):** persist the row in its initial state
  (`status="queued"`), `await db.commit()` so the Modal worker can see it, then
  dispatch via `call_assessment_runner(...)` / `spawn.aio(...)`; serialize
  concurrent duplicate requests with the existing advisory-lock + duplicate-check
  pattern, and on dispatch failure mark the row `failed` in a fresh transaction.
  Follow this flow rather than reimplementing it divergently.
- **Authorization scoping:** non-admin list/read endpoints filter through
  `UserGroup` → `BibleVersionAccess` (push the filter into SQL as a subquery;
  see the lexeme-card and assessment handlers) rather than materializing large
  Python `IN` lists. Admins get the unfiltered query.
- **Soft deletes:** "delete" sets `deleted = True` / `deletedAt`; reads filter
  `deleted.is_(False)` / `deleted.is_not(True)`. Don't hard-delete rows that the
  rest of the codebase soft-deletes.
- **Logging:** use `logger = setup_logger(__name__, container_id=container_id)`
  (with `container_id = socket.gethostname()`) and log meaningful start/error
  events with structured `extra={...}`, matching sibling modules.
- **CORS:** respect the allowlist built in `app.py` (`DEFAULT_ALLOWED_ORIGINS` +
  `ALLOWED_ORIGINS`); do not loosen it or re-add wildcard-with-credentials.
- Reuse shared helpers in `utils/` (`logging_config`, `verse_range_utils`,
  `morpheme_tokenizer`) and `security_routes/utilities.py` instead of
  reimplementing them.

## 3. Design patterns & SOLID principles

Review for maintainable, well-structured code:

- **Single Responsibility:** route handlers should orchestrate (validate → check
  auth → query/persist → dispatch → respond), not accrete deep business logic.
  Push reusable logic into module-level helpers (e.g. `_apply_filters`,
  `_apply_lexeme_card_patch`, `_effective_source_version_expr`), `utils/`, or
  `security_routes/utilities.py`. Flag handlers that grow into very large
  procedural blocks with duplicated branches.
- **DRY:** flag copy-pasted authorization checks, duplicate-detection logic, or
  response-shaping that should be extracted into a shared helper. Auth scoping
  (groups → version access) and the assessment duplicate/advisory-lock logic in
  particular should not be duplicated divergently across handlers.
- **Open/Closed & extensibility:** prefer parametrizing behavior (e.g. `ListMode`
  enum, `Enum`/`Literal` query params, optional pagination) over branching that
  requires editing many call sites. New assessment types should slot into the
  existing type/validation tables, not bespoke conditionals scattered around.
- **Dependency Inversion / Interface Segregation:** rely on FastAPI dependency
  injection (`Depends`) for auth (`get_current_user`) and the DB session
  (`get_db`) rather than constructing engines/sessions inside handlers; read
  configuration from env (`os.getenv`) at module scope as the existing code does.
- **Typed contracts & consistent shapes:** keep request/response shapes typed
  with Pydantic models, validate with `model_validate`, and ensure identical
  concepts have identical shapes across endpoints (same response keys, same
  status codes, same `*Out` model for the same resource).
- **Schema changes go through alembic:** any change to `database/models.py`
  (new column, table, index, constraint) needs a matching alembic migration in
  `alembic/migrations/versions/` whose `down_revision` points at the current
  head. Flag model changes that ship without a migration.

## 4. What NOT to flag

- Code style, import ordering, and formatting — these are enforced by **black**,
  **isort** (profile black), and **flake8** via the `linting` Make target and
  pre-commit hooks. Note that flake8 ignores `E501` (line length) and excludes
  `v1/`, `v2/`, and `alembic/`, so do not nitpick long lines or legacy-version
  style.
- Pre-existing patterns that the PR merely follows (review the diff, not the
  whole repo's legacy choices) — including frozen v1/v2 modules.
- Generated migration files under `alembic/` and test `fixtures/` (and the `test/`
  scaffolding), beyond confirming a needed migration exists.
- Local-imports-inside-handlers (e.g. `from sqlalchemy import select` inside a
  `try`) — this is an established convention in this codebase, not a defect.

Keep findings focused and actionable. Prioritize correctness, security, and
consistency over stylistic preferences.
