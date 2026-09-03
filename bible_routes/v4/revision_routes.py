"""v4 Revisions router (issue #891, epic #842).

The second Bible-domain slice, and the structural prerequisite for Verses & text
(#892), whose collection is revision-scoped (``GET /v4/revisions/{id}/verses``). Five
endpoints:

* ``GET    /v4/revisions``      — paginated list (``V4Page[RevisionOut]``), optional
  ``version_id`` filter.
* ``GET    /v4/revisions/{id}`` — single revision; 404 ``REVISION_NOT_FOUND``. New in
  v4: v3 had no GET-one.
* ``POST   /v4/revisions``      — create + load verse text. Synchronous **201**, single
  JSON body (#826) — no multipart, no ``?query=`` fields.
* ``PATCH  /v4/revisions/{id}`` — partial field update, replacing v3's
  ``PUT /revision?id=&new_name=``.
* ``DELETE /v4/revisions/{id}`` — soft-delete (**204**); v3 returned 200 with a prose
  body.

Resource-style plural URL (#825). Auth is applied at the router level in
:func:`api_v4.app.create_v4_app` (#831, protected-by-default), so these handlers only
re-declare ``current_user`` when they need it (FastAPI dedupes the dependency).

This module owns HTTP concerns only: it calls :mod:`bible_routes.v4.revision_service`
for all data access and maps that service's domain signals onto the #828
:class:`~api_v4.errors.V4APIError` envelope. The error ``code`` values are the stable
contract clients branch on. The authorization and transaction decisions — including the
three v3 behaviors #891 says not to port, and why a soft-deleted version hides its
revisions — live in the service's module docstring, since they are not HTTP concerns.

Delta sync (``updated_since`` / ``updated_at`` / ``next_updated_since``) is here, and
is deliberately character-for-character the same contract as ``GET /v4/versions`` — the
whole point of settling #899 before adding it to a second list was that both lists then
say the same thing. :mod:`api_v4.delta` owns that contract; this module only wires it.
"""

__version__ = "v4"

from datetime import datetime
from typing import Optional

import fastapi
from fastapi import Depends, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.delta import next_watermark, updated_since_description
from api_v4.errors import V4_FORBIDDEN_RESPONSE, V4APIError, error_responses
from api_v4.pagination import PaginationParams, V4Page
from api_v4.schemas.bible import RevisionCreate, RevisionOut, RevisionPatch
from bible_routes.v4 import revision_service
from database.dependencies import get_db
from database.models import BibleRevision, BibleVersion
from database.models import UserDB as UserModel
from security_routes.v4.dependencies import get_current_user_v4

router = fastapi.APIRouter(prefix="/revisions", tags=["Revisions"])


def _to_out(revision: BibleRevision, version: BibleVersion | None) -> RevisionOut:
    """Build the snake_case ``RevisionOut`` from an ORM row + its parent version.

    Built from **named columns**, not from ``revision.__dict__`` — the first of the
    three v3 behaviors #891 says not to port (see :class:`RevisionOut`). This is also
    the one place the wire names are bridged to the ORM spellings
    (``bible_version_id`` -> ``version_id``, ``back_translation_id`` ->
    ``back_translation``) and the two denormalized parent fields are attached.

    Booleans are coerced with ``bool(...)`` because their columns are nullable and
    legacy rows may hold NULL (mirroring v3's null-to-false coercion for ``deleted``).

    ``date`` is narrowed to a date here rather than by Pydantic coercion, which *raises*
    on a datetime whose time component is not midnight — and the column is a DateTime,
    so a legacy row is not guaranteed to be. The ``isinstance`` check is what makes this
    total over both shapes the attribute can hold: a row read back from Postgres carries
    a ``datetime``, but an ORM object built in Python and not yet round-tripped still
    holds whatever was assigned (v3's upload assigns a ``date``). Narrowing only the
    datetime case means neither can turn a response into a 500.
    """
    revision_date = revision.date
    if isinstance(revision_date, datetime):
        revision_date = revision_date.date()

    return RevisionOut(
        id=revision.id,
        version_id=revision.bible_version_id,
        name=revision.name,
        date=revision_date,
        published=bool(revision.published),
        back_translation=revision.back_translation_id,
        machine_translation=bool(revision.machine_translation),
        deleted=bool(revision.deleted),
        version_abbreviation=version.abbreviation if version else None,
        iso_language=version.iso_language if version else None,
        # Not coerced: NULL is meaningful (a legacy row predating the column) and the
        # field is Optional on the wire, so it passes through — same as VersionOut.
        updated_at=revision.updated_at,
    )


async def _out_for(db: AsyncSession, revision: BibleRevision) -> RevisionOut:
    """Load one revision's parent version and shape the response.

    The single-row counterpart to the batch load the list endpoint does, so all four
    body-returning handlers build their response through :func:`_to_out` with the parent
    attached the same way.
    """
    version_map = await revision_service.versions_for_revisions(db, [revision])
    return _to_out(revision, version_map.get(revision.bible_version_id))


def _version_not_visible_error(exc, version_id: int) -> V4APIError:
    """Map :class:`revision_service.VersionNotVisible` onto its V4APIError.

    Shared by list and create because both name a parent version and must report an
    unusable one identically. The code is ``VERSION_NOT_FOUND`` — the same code
    ``/v4/versions`` uses, since it is the same fact about the same resource — and it
    covers unknown, inaccessible and soft-deleted alike (see the service).
    """
    return V4APIError(
        status_code=status.HTTP_404_NOT_FOUND,
        code="VERSION_NOT_FOUND",
        message=str(exc),
        details={"version_id": version_id},
    )


def _invalid_reference_error(exc) -> V4APIError:
    """Map :class:`revision_service.InvalidReference` onto its V4APIError.

    Shared by create and patch: a ``back_translation`` id that does not exist is the
    same client mistake on either verb, and the #828 point is that it gets a stable 4xx
    code rather than falling through to the catch-all 500.
    """
    return V4APIError(
        status_code=status.HTTP_400_BAD_REQUEST,
        code="INVALID_REFERENCE",
        message=(
            "A referenced value does not exist. Check the FK-backed fields: "
            "back_translation."
        ),
        details={"fields": list(revision_service.InvalidReference.FIELDS)},
    )


def _write_gate_error(exc, revision_id: int) -> V4APIError:
    """Map a :func:`revision_service._get_revision_for_write` signal to a V4APIError.

    Shared by patch and delete, which authorize through the same gate and therefore
    raise the same two signals with the same codes. An unrecognized signal is
    **re-raised unchanged** rather than guessed at: it then reaches the #828 catch-all as
    a 500, which is the correct, loud outcome for a contract gap — inventing a 4xx code
    for it would hide the omission.
    """
    if isinstance(exc, revision_service.RevisionNotFound):
        return V4APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="REVISION_NOT_FOUND",
            message=str(exc),
            details={"revision_id": revision_id},
        )
    if isinstance(exc, revision_service.RevisionAccessForbidden):
        return V4APIError(
            status_code=status.HTTP_403_FORBIDDEN,
            code="REVISION_ACCESS_FORBIDDEN",
            message=str(exc),
            details={"revision_id": revision_id},
        )
    raise exc


@router.get("", response_model=V4Page[RevisionOut])
async def list_revisions(
    page: PaginationParams = Depends(),
    # Optional[...] rather than `int | None` only because black splits the union
    # across lines when it carries a Query() default; identical semantics.
    version_id: Optional[int] = Query(
        None,
        description=(
            "Return only revisions of this version. An unknown, inaccessible or "
            "soft-deleted version id is a 404 VERSION_NOT_FOUND rather than an empty "
            "page, so a mistyped id cannot look like a version with no revisions."
        ),
    ),
    include_deleted: bool = Query(
        False,
        description=(
            "Admins only (ignored for other callers): also return soft-deleted "
            "revisions, and revisions whose version is soft-deleted. Cannot be "
            "combined with version_id to reach a soft-deleted version — filtering by "
            "version requires a visible one."
        ),
    ),
    updated_since: Optional[datetime] = Query(
        None,
        description=updated_since_description(
            "revisions",
            delta_also=(
                ", including when a revision's parent version was soft-deleted"
            ),
            cannot_carry=(
                "No watermark can carry a hard-deleted row (it never enters any "
                "window), nor a revision that became visible because its version was "
                "granted to one of your groups — that grant moves the *version's* "
                "watermark, not its revisions'."
            ),
        ),
    ),
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> V4Page[RevisionOut]:
    """List revisions the caller may access, lowest id first, paginated.

    A revision is visible when its parent version is visible to the caller — read
    access is granted per version, via group membership — and neither the revision nor
    its version is soft-deleted.

    ``updated_since`` turns the same list into a delta feed, and every response carries
    ``next_updated_since`` — the caller's next watermark, lapped server-side by
    :func:`api_v4.delta.next_watermark` (#899). Deliberately identical to
    ``GET /v4/versions``: one contract, so a mirror walks either list the same way.
    """
    try:
        revisions, total, max_updated_at = await revision_service.list_revisions(
            db,
            current_user,
            limit=page.limit,
            offset=page.offset,
            version_id=version_id,
            include_deleted=include_deleted,
            updated_since=updated_since,
        )
    except revision_service.VersionNotVisible as exc:
        raise _version_not_visible_error(exc, version_id) from exc

    version_map = await revision_service.versions_for_revisions(db, revisions)
    items = [_to_out(r, version_map.get(r.bible_version_id)) for r in revisions]
    return V4Page[RevisionOut].create(
        items=items,
        total=total,
        pagination=page,
        next_updated_since=next_watermark(max_updated_at),
    )


@router.get("/{revision_id}", response_model=RevisionOut)
async def get_revision(
    revision_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> RevisionOut:
    """Fetch a single revision by id, scoped to what the caller may see.

    New in v4 — v3 offered no single-revision read, only the full list. A revision the
    caller cannot see reports the same 404 as one that does not exist.
    """
    try:
        revision = await revision_service.get_revision(db, current_user, revision_id)
    except revision_service.RevisionNotFound as exc:
        raise V4APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code="REVISION_NOT_FOUND",
            message=str(exc),
            details={"revision_id": revision_id},
        ) from exc
    return await _out_for(db, revision)


@router.post(
    "",
    response_model=RevisionOut,
    status_code=status.HTTP_201_CREATED,
    # 400 is reachable here but not on most of the surface, so it is declared on the
    # route rather than in the shared set: an unresolvable foreign key in the body.
    responses=error_responses(status.HTTP_400_BAD_REQUEST),
)
async def create_revision(
    data: RevisionCreate,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> RevisionOut:
    """Create a revision and load its verse text, in one JSON request.

    The verse text arrives in the body as base64-encoded, vref-aligned UTF-8 plaintext
    (`text.type: "inline"`) — there is no multipart upload on the v4 surface (#826). The
    `text.type` discriminator is where a future S3-reference source is added, so a client
    sending `"inline"` today keeps working.

    **Responds 201 with the created revision.** Uploading a full Bible takes seconds, not
    the hours that make a job envelope worthwhile, so this is not a polling endpoint.
    Clients should nonetheless be written to accept **either** a `201` with the resource
    body **or** a `202` with a `Location` header pointing at a job to poll: a
    sufficiently large upload may be deferred in future, and saying so now means adding
    that path later is not a breaking change. The pressure that would motivate it is
    connection-pool occupancy — a synchronous upload holds a pooled DB connection for its
    whole duration, so heavy concurrent upload traffic is the plausible trigger.

    The revision row and every verse row commit in a single transaction, so a failure —
    malformed text, a bad reference, or a client disconnect mid-upload — leaves no
    partially-loaded revision behind.

    Authorization matches v3: group access to the parent version is enough, the caller
    need not own it. See the service for why that differs from the patch/delete gate.
    """
    try:
        revision = await revision_service.create_revision(db, current_user, data)
    except revision_service.VersionNotVisible as exc:
        raise _version_not_visible_error(exc, data.version_id) from exc
    except revision_service.InvalidVerseText as exc:
        raise V4APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code="INVALID_VERSE_TEXT",
            message=str(exc),
            details=exc.details,
        ) from exc
    except revision_service.InvalidReference as exc:
        raise _invalid_reference_error(exc) from exc
    return await _out_for(db, revision)


@router.patch(
    "/{revision_id}",
    response_model=RevisionOut,
    # 400 is reachable here but not on most of the surface, so it is declared on the
    # route rather than in the shared set: an unresolvable foreign key in the body.
    # 403 is declared per write rather than shared: v4 answers 404 for a resource
    # the caller cannot see, so 403 only ever means "visible, but not yours".
    # See V4_ERROR_RESPONSES.
    responses={
        **error_responses(status.HTTP_400_BAD_REQUEST),
        **V4_FORBIDDEN_RESPONSE,
    },
)
async def update_revision(
    revision_id: int,
    data: RevisionPatch,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> RevisionOut:
    """Partially update a revision (parent version's owner, or an admin).

    Replaces v3's ``PUT /revision?id=&new_name=``: the new name is a body field rather
    than a query parameter, the response is the updated resource rather than a prose
    ``{"detail": ...}`` message, and the same closed allowlist covers the other mutable
    fields (``published``, ``back_translation``, ``machine_translation``). A
    non-patchable field such as ``version_id`` or ``deleted`` is a 422 from the
    ``RevisionPatch`` allowlist rather than something this handler has to strip.
    """
    try:
        revision = await revision_service.update_revision(
            db, current_user, revision_id, data
        )
    except revision_service.InvalidReference as exc:
        raise _invalid_reference_error(exc) from exc
    except revision_service.RevisionServiceError as exc:
        raise _write_gate_error(exc, revision_id) from exc
    return await _out_for(db, revision)


@router.delete(
    "/{revision_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    # 403 is declared per write rather than shared: v4 answers 404 for a resource
    # the caller cannot see, so 403 only ever means "visible, but not yours".
    # See V4_ERROR_RESPONSES.
    responses=V4_FORBIDDEN_RESPONSE,
)
async def delete_revision(
    revision_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: UserModel = Depends(get_current_user_v4),
) -> Response:
    """Soft-delete a revision (parent version's owner, or an admin).

    204 with no body, where v3 returned 200 and a prose ``{"detail": ...}``. The
    soft-delete itself is unchanged: the row and its verses stay, ``deleted`` flips.
    Idempotent — re-deleting an already-deleted revision is another 204.
    """
    try:
        await revision_service.soft_delete_revision(db, current_user, revision_id)
    except revision_service.RevisionServiceError as exc:
        raise _write_gate_error(exc, revision_id) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)
