"""Revision data-access service for the v4 surface (issue #891, epic #842).

The second Bible-domain slice, following the shape :mod:`bible_routes.v4.version_service`
established: functions here take an :class:`~sqlalchemy.ext.asyncio.AsyncSession`, the
current :class:`~database.models.UserDB`, and plain data; they return ORM rows and raise
the small :class:`RevisionServiceError` signals below. They know nothing about HTTP
status codes or the v4 error envelope — the router (``revision_routes.py``) maps each
signal onto a :class:`api_v4.errors.V4APIError`. v3 (``bible_routes/v3/revision_routes.py``)
is the behavioral spec and stays frozen and untouched.

Authorization, and where it comes from:

* **Read visibility** (list / get) is *derived from the parent version*, not defined
  again here: a revision is visible exactly when its ``bible_version`` is visible to
  the caller and the revision itself is not soft-deleted. The version half of that is
  delegated to :func:`version_service.get_version` for a single-version lookup, and
  mirrors its predicate (group access via ``bible_version_access``, admins see
  everything) for the list. Authorization for two resources in one hierarchy must have
  one definition or the two drift.
* **Write** (patch / delete) is the parent version's **owner or an admin** — v3 parity:
  v3's rename and delete both authorize against ``bible_version.owner_id``, never
  against group membership. Shared via :func:`_get_revision_for_write` so both write
  paths authorize identically.
* **Create** is *group access* to the parent version, not ownership — also v3 parity
  (v3 calls ``is_user_authorized_for_bible_version``). So any member of a group that can
  see a version may add a revision to it, while only the version's owner may then rename
  or delete that revision. That asymmetry is inherited deliberately rather than
  smoothed over: narrowing create to owners would remove an ability v3 users have.

**A soft-deleted version hides its revisions** (issue #891 decision, a deliberate
divergence from v3). v3 filters only ``bible_revision.deleted``, so soft-deleting a
version leaves its revisions listable — the version vanishes from ``/version`` while its
children remain. v4 filters on both, which makes the three read paths agree: if
``GET /v4/versions/{id}`` 404s, so do that version's revisions. Consequences, stated
because the divergence is the kind of thing that looks like a bug later:

* ``include_deleted`` (admin-only, as on versions) lifts **both** filters, so no row
  becomes unreachable — an admin can always still see it.
* The write gate does *not* filter either flag (see :func:`_get_revision_for_write`), so
  an owner can still rename or re-delete a revision under a deleted version. Only the
  read paths hide it.
* Creating a revision under a soft-deleted version is therefore a **404**, not v3's
  400 "Version is deleted": the create path resolves the parent through the same
  visibility rule as a read, so a deleted version is simply not there. There is no
  separate "deleted" signal to report, and inventing one would mean looking the version
  up globally by id — which would leak the existence of versions the caller cannot see.

**One transaction for the whole upload** (:func:`create_revision`). The revision row and
every ``verse_text`` row commit together, so a failure — a malformed payload, a bad FK, a
client disconnect — leaves no half-loaded revision. v3 already had this shape and v4
keeps it; what v4 adds is the ``BaseException`` guard, because
:class:`asyncio.CancelledError` (how a client disconnect arrives) is *not* an
``Exception`` and v3's ``except Exception`` therefore never rolled back for it. That
previously relied on ``get_db``'s ``close()`` rolling the transaction back implicitly —
which it does, but #748 lists that as unverified cleanup, so this module rolls back
explicitly and ``test_revision_routes_v4`` pins the outcome.

Two v3 defects in the write paths, not carried over:

* v3's ``DELETE /revision`` and ``PUT /revision`` both do
  ``revision, bible_version = result.first()``. On an unknown id ``.first()`` returns
  ``None``, so the unpacking raises ``TypeError`` and the request 500s *before* reaching
  the ``if not revision`` branch that was meant to report it. Both of v3's
  missing-revision error paths are unreachable. :func:`_get_revision_for_write` checks
  for the absent row before destructuring it.
* v3's rename and delete then test ``bible_version is None`` — impossible, since the row
  came from an inner join on ``bible_version``. The join is retained (the owner id lives
  there and is needed for the gate); the dead branch is not.
"""

import base64
import binascii
from datetime import date

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from bible_loading import async_text_dataframe, text_loading
from bible_routes.v4 import version_service
from database.models import (
    BibleRevision,
    BibleVersion,
    BibleVersionAccess,
    UserDB,
    UserGroup,
)


class RevisionServiceError(Exception):
    """Base for revision-service domain signals the router maps to V4APIError."""


class RevisionNotFound(RevisionServiceError):
    """No revision with this id is visible to (list/get) or reachable by the caller."""

    def __init__(self, revision_id: int) -> None:
        self.revision_id = revision_id
        super().__init__(f"Revision {revision_id} does not exist.")


class RevisionAccessForbidden(RevisionServiceError):
    """Caller is neither the parent version's owner nor an admin (patch / delete)."""

    def __init__(self, revision_id: int) -> None:
        self.revision_id = revision_id
        super().__init__(f"Not authorized to modify revision {revision_id}.")


class VersionNotVisible(RevisionServiceError):
    """The parent version named by a create or a ``version_id`` filter is not visible.

    Covers "no such version", "not accessible to this caller", and "soft-deleted"
    with one signal, because the read predicate cannot tell them apart and must not:
    reporting them separately would tell a caller which version ids exist. v3 split
    them into a 400 (unknown), a 403 (unauthorized) and a 400 (deleted).
    """

    def __init__(self, version_id: int) -> None:
        self.version_id = version_id
        super().__init__(f"Version {version_id} does not exist.")


class InvalidReference(RevisionServiceError):
    """A create or patch referenced a value that fails a FK constraint on write.

    ``BibleRevision.back_translation_id`` is a FK to ``bible_revision.id``, so a
    non-existent id raises an ``IntegrityError`` on flush. The router maps this to a
    stable 4xx (``INVALID_REFERENCE``) rather than letting it reach the #828 catch-all
    as a 500.

    Note the parent version id is *not* in this set even though it is also a FK: it is
    resolved and authorized before any write, so a bad ``version_id`` is already a
    :class:`VersionNotVisible` 404 and can never reach the flush.
    """

    #: FK-backed request fields, surfaced in the error details as a hint about which
    #: inputs can trigger this.
    FIELDS = ("back_translation",)


class InvalidVerseText(RevisionServiceError):
    """The uploaded text is not decodable, or is not vref-aligned.

    Three causes, all client input and all therefore a stable 400 rather than a 500:
    the base64 does not decode, the bytes are not UTF-8, or the line count does not
    match the vref skeleton (``fixtures/vref.txt``, 41,899 lines). ``details`` carries
    machine-readable context for the caller (#828 keeps prose in ``message``).
    """

    def __init__(self, message: str, details: dict | None = None) -> None:
        self.details = details or {}
        super().__init__(message)


def _visible_revisions_query(user: UserDB, *, include_deleted: bool):
    """Base ``SELECT BibleRevision`` scoped to what ``user`` may see.

    No ``limit``/``offset``/``order_by`` — callers add those (and the count query wraps
    this as a subquery), so the filter/authorization logic lives in exactly one place.

    The join to ``bible_version`` is unconditional because visibility depends on the
    parent on both branches: an admin still gets the parent's ``deleted`` filter, and a
    non-admin additionally needs the ``bible_version_access`` join. ``IS NOT TRUE``
    rather than ``IS FALSE`` on both ``deleted`` columns: they are nullable and legacy
    rows may hold NULL, which the response layer coerces to ``False`` — so a NULL row
    must stay *visible* rather than silently vanish from listings (the same v4
    refinement ``version_service`` documents).
    """
    stmt = select(BibleRevision).join(
        BibleVersion, BibleVersion.id == BibleRevision.bible_version_id
    )

    if not user.is_admin:
        # Only revisions whose version is reachable through a group the user belongs
        # to. distinct() because a version accessible via two of the caller's groups
        # matches the join twice.
        stmt = (
            stmt.distinct()
            .join(
                BibleVersionAccess,
                BibleVersionAccess.bible_version_id == BibleVersion.id,
            )
            .where(
                BibleVersionAccess.group_id.in_(
                    select(UserGroup.group_id).where(UserGroup.user_id == user.id)
                ),
            )
        )

    if not include_deleted:
        # Both halves: a soft-deleted revision, and any revision of a soft-deleted
        # version. See the module docstring for why the parent's flag counts.
        stmt = stmt.where(
            BibleRevision.deleted.is_not(True),
            BibleVersion.deleted.is_not(True),
        )
    return stmt


async def _require_visible_version(
    db: AsyncSession, user: UserDB, version_id: int
) -> BibleVersion:
    """Resolve a parent version the caller may see, or raise :class:`VersionNotVisible`.

    Delegates to :func:`version_service.get_version` instead of re-deriving the
    predicate, so "a version this caller may see" has one implementation across the v4
    surface. Its ``VersionNotFound`` is re-signalled as this module's
    :class:`VersionNotVisible` so the router maps revision-endpoint errors from one
    exception family.
    """
    try:
        return await version_service.get_version(db, user, version_id)
    except version_service.VersionNotFound as exc:
        raise VersionNotVisible(version_id) from exc


async def list_revisions(
    db: AsyncSession,
    user: UserDB,
    *,
    limit: int,
    offset: int,
    version_id: int | None = None,
    include_deleted: bool = False,
) -> tuple[list[BibleRevision], int]:
    """Return one page of revisions the user may see, plus the total match count.

    ``version_id`` narrows the page to one version's revisions. It is validated
    *before* being used as a filter — an unknown, inaccessible or soft-deleted version
    raises :class:`VersionNotVisible` rather than quietly returning an empty page, so a
    typo'd id cannot look like "this version has no revisions". (v3 reported the same
    situation as a 400 for an unknown id and a 403 for an unauthorized one; see
    :class:`VersionNotVisible` for why v4 collapses them.)

    ``include_deleted`` is honored only for admins, as on ``GET /v4/versions``; a
    non-admin never receives soft-deleted rows regardless of the flag. Note it is
    checked *after* the ``version_id`` lookup, which always applies the non-deleted
    predicate — so ``?version_id=X&include_deleted=true`` on a soft-deleted version is
    a 404 even for an admin. Filtering by a version means naming a visible one; an
    admin who wants deleted rows across the board omits the filter.

    ``total`` counts *all* matching rows ignoring ``limit``/``offset`` (for the
    pagination envelope), computed from the same scoped query as the page. They are
    still two statements, so a concurrent write between them can cause the usual (rare)
    offset-pagination drift between ``total`` and ``len(items)``.
    """
    if version_id is not None:
        await _require_visible_version(db, user, version_id)

    stmt = _visible_revisions_query(
        user, include_deleted=include_deleted and user.is_admin
    )
    if version_id is not None:
        stmt = stmt.where(BibleRevision.bible_version_id == version_id)

    total = (
        await db.execute(select(func.count()).select_from(stmt.subquery()))
    ).scalar_one()
    result = await db.execute(
        stmt.order_by(BibleRevision.id).limit(limit).offset(offset)
    )
    return list(result.scalars().all()), total


async def get_revision(
    db: AsyncSession, user: UserDB, revision_id: int
) -> BibleRevision:
    """Return a single revision the user may see, or raise :class:`RevisionNotFound`.

    Visibility-scoped: a caller asking for a revision they cannot see gets the same
    ``RevisionNotFound`` as a truly missing id, so existence is not leaked. (There is no
    v3 GET-one endpoint to mirror; this is the secure default, matching
    :func:`version_service.get_version`.)
    """
    stmt = _visible_revisions_query(user, include_deleted=False).where(
        BibleRevision.id == revision_id
    )
    revision = (await db.execute(stmt)).scalars().first()
    if revision is None:
        raise RevisionNotFound(revision_id)
    return revision


def decode_verse_text(content_base64: str) -> list[str | None]:
    """Decode an :class:`~api_v4.schemas.bible.InlineText` payload into verse lines.

    Returns one entry per line of the uploaded text: the line itself, or ``None`` for a
    blank/whitespace-only line (a vref with no verse). That is the shape
    :func:`bible_loading.async_text_dataframe` expects, and it matches v3's parse
    (``process_and_upload_revision``) line for line — reimplemented here rather than
    imported, so v4 does not depend on a frozen v3 route module. ``bible_loading``
    itself, being shared and v3's upload hot path, is called and not modified.

    Raises :class:`InvalidVerseText` for undecodable base64, non-UTF-8 bytes, or text
    with no verses at all. The *line-count* check is not here: it belongs to
    ``bible_loading``, which owns the vref skeleton, so it surfaces from
    :func:`create_revision` instead.

    ``validate=False`` (the default) makes the decoder ignore characters outside the
    base64 alphabet, so line-wrapped base64 — what most encoders emit for a payload
    this size — is accepted. The cost is that a wholly non-base64 body is not rejected
    *as* bad base64; it decodes to garbage and fails at the UTF-8 or line-count check
    instead. Either way the caller gets INVALID_VERSE_TEXT, and tolerating wrapping is
    worth more than pinpointing which of the three checks a malformed payload trips.
    """
    try:
        raw = base64.b64decode(content_base64)
    except (binascii.Error, ValueError) as exc:
        raise InvalidVerseText(
            "text.content_base64 is not valid base64.",
            {"field": "text.content_base64"},
        ) from exc

    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InvalidVerseText(
            "Decoded verse text is not valid UTF-8.",
            {"field": "text.content_base64", "encoding": "utf-8"},
        ) from exc

    verses: list[str | None] = []
    has_text = False
    for line in text.splitlines():
        if line.strip():
            verses.append(line)
            has_text = True
        else:
            verses.append(None)

    if not has_text:
        raise InvalidVerseText(
            "Decoded verse text contains no verses.",
            {"field": "text.content_base64", "lines": len(verses)},
        )
    return verses


async def create_revision(db: AsyncSession, user: UserDB, data) -> BibleRevision:
    """Create a revision under a visible version and load its verse text.

    ``data`` is a ``RevisionCreate``. Synchronously loads the whole upload and returns
    the committed row — the endpoint is a 201, not a job (see the router).

    Order: authorize the parent version, then decode the payload, then write. So the
    parent-version check and the base64/UTF-8 checks do happen before any row is
    inserted — but the **vref line-count check does not**. That one belongs to
    ``bible_loading``, which owns the vref skeleton, so it runs *after* the revision row
    is flushed. It is safe rather than sloppy only because of the single transaction
    below, and ``test_wrong_line_count_leaves_no_revision_behind`` pins the outcome.

    One transaction covers the revision row and every verse: ``flush`` assigns the id
    the verse rows' FK needs without ending the transaction, the verses are inserted in
    batches, and a single ``commit`` ends it. So there is no observable state in which a
    revision exists with a partial set of verses — including when the client
    disconnects, which arrives as :class:`asyncio.CancelledError` and is caught by the
    ``BaseException`` guard (see the module docstring for why ``except Exception`` was
    not enough).

    The write is split into **two ``try`` blocks sharing that one transaction**, purely
    so the error mapping can be precise about whose fault a failure is:

    * The **revision flush** maps ``IntegrityError`` to :class:`InvalidReference`. Sound
      because ``back_translation`` is the only client-supplied FK on that row —
      ``bible_version_id`` was already resolved and authorized above — so it is the only
      constraint a client can break here.
    * The **verse inserts** deliberately carry *no* ``IntegrityError`` translation.
      ``verse_text.verse_reference`` is a FK to ``verse_reference.full_verse_id``, so a
      failure there means the reference table no longer matches ``fixtures/vref.txt`` —
      server-side data drift, not client input. Mapping it to a 400
      ``INVALID_REFERENCE`` would tell the client to fix ``back_translation``, which is
      not the problem, and would bury a condition that ought to page someone. It falls
      through to the #828 catch-all 500 on purpose: **do not add a handler here.**

    Raises :class:`VersionNotVisible` (unknown / inaccessible / soft-deleted parent),
    :class:`InvalidVerseText` (undecodable or non-vref-aligned text) and
    :class:`InvalidReference` (a ``back_translation`` id that does not exist).
    """
    await _require_visible_version(db, user, data.version_id)
    verses = decode_verse_text(data.text.content_base64)

    new_revision = BibleRevision(
        bible_version_id=data.version_id,
        name=data.name,
        date=date.today(),
        published=data.published,
        back_translation_id=data.back_translation,
        machine_translation=data.machine_translation,
    )
    # Stage 1: the revision row only. Scoping the IntegrityError translation to this
    # flush is what keeps INVALID_REFERENCE meaning "a field you sent is wrong".
    try:
        db.add(new_revision)
        await db.flush()
    except IntegrityError as exc:
        # Client input referenced a non-existent FK target (back_translation).
        await db.rollback()
        raise InvalidReference() from exc
    except BaseException:
        # See the guard on stage 2 for why this is BaseException.
        await db.rollback()
        raise

    # Stage 2: the verses, and the commit that ends the transaction stage 1 opened.
    # No IntegrityError branch here on purpose — a verse_reference FK failure is
    # server-side reference drift, not client input, and must surface as a 500. See
    # the docstring.
    try:
        verse_records = await async_text_dataframe(verses, new_revision.id)
        await text_loading(verse_records, db)
        await db.commit()
    except ValueError as exc:
        # bible_loading rejects text that is not one line per vref. Its message
        # already names the expected and actual line counts, so it is the clearest
        # thing to report — it is our own text, not the client's.
        await db.rollback()
        raise InvalidVerseText(str(exc), {"field": "text.content_base64"}) from exc
    except BaseException:
        # BaseException, not Exception: a client disconnect cancels the handler task
        # with asyncio.CancelledError, which is a BaseException. Rolling back here
        # rather than relying on get_db's close() is what makes the no-half-loaded-
        # revision guarantee explicit instead of incidental (#748).
        await db.rollback()
        raise
    await db.refresh(new_revision)
    return new_revision


async def _get_revision_for_write(
    db: AsyncSession, user: UserDB, revision_id: int
) -> BibleRevision:
    """Load a revision for a write, enforcing the owner-or-admin gate.

    Shared by :func:`update_revision` and :func:`soft_delete_revision` so both
    authorize identically and one reading of "may modify" exists. "Owner" means the
    *parent version's* ``owner_id`` — v3 parity; a revision has no owner column of its
    own.

    The lookup is global by id and **not** visibility-scoped, mirroring
    :func:`version_service._get_version_for_write`: a caller who is neither owner nor
    admin gets :class:`RevisionAccessForbidden` rather than :class:`RevisionNotFound`,
    even for a revision they cannot see. That does tell them the id exists — a
    deliberate v3-parity choice kept consistent across the write endpoints, unlike the
    read path, which hides existence behind a 404.

    It also does not filter either ``deleted`` flag, so an already-soft-deleted revision
    (or one whose version is soft-deleted) stays writable. Two reasons, matching the
    version service: :func:`soft_delete_revision` documents itself as idempotent, which
    requires this gate to load an already-deleted row; and hiding a row from *writes*
    because its parent is deleted would leave an owner unable to touch rows they still
    own. ``deleted`` is not a ``RevisionPatch`` field, so no write here can resurrect a
    revision.

    The join to ``bible_version`` is an inner join, so a revision whose
    ``bible_version_id`` is NULL or dangling reports as :class:`RevisionNotFound`. There
    is a FK on the column, so that is a shape the database does not permit — which is
    why v3's ``bible_version is None`` branch was dead code, not a guard worth keeping.
    """
    row = (
        await db.execute(
            select(BibleRevision, BibleVersion.owner_id).join(
                BibleVersion, BibleVersion.id == BibleRevision.bible_version_id
            )
            # Check for the absent row before destructuring it — v3 unpacked
            # .first() unconditionally and 500'd on an unknown id.
            .where(BibleRevision.id == revision_id)
        )
    ).first()
    if row is None:
        raise RevisionNotFound(revision_id)
    revision, owner_id = row
    if not user.is_admin and owner_id != user.id:
        raise RevisionAccessForbidden(revision_id)
    return revision


#: Patchable ``RevisionPatch`` field -> ``BibleRevision`` ORM attribute. Exhaustive
#: over the schema's fields, and :func:`update_revision` indexes it *directly* (no
#: ``.get``): a field added to ``RevisionPatch`` without a mapping must fail loudly
#: instead of being silently dropped — the failure mode behind v3's phantom
#: ``is_reference``. ``test_revision_routes_v4`` pins the two together.
_PATCH_FIELD_TO_COLUMN = {
    "name": "name",
    "published": "published",
    # The one request field whose ORM attribute is spelled differently.
    "back_translation": "back_translation_id",
    "machine_translation": "machine_translation",
}


async def update_revision(
    db: AsyncSession, user: UserDB, revision_id: int, data
) -> BibleRevision:
    """Partially update a revision's fields in one transaction (owner or admin).

    ``data`` is a ``RevisionPatch``; only fields the client actually sent are written
    (``exclude_unset``), mapped through :data:`_PATCH_FIELD_TO_COLUMN`. Replaces v3's
    ``PUT /revision?id=&new_name=``, which could only rename and reported success as a
    prose ``{"detail": ...}`` body; this returns the updated resource.

    An empty patch (``{}``) is a no-op returning the current row without issuing an
    ``UPDATE``. Same for a patch that only re-sends values the row already holds —
    SQLAlchemy emits no ``UPDATE`` when no attribute actually changes.

    Raises :class:`RevisionNotFound` / :class:`RevisionAccessForbidden` from the shared
    gate, and :class:`InvalidReference` when a patched ``back_translation`` points at a
    revision that does not exist.
    """
    revision = await _get_revision_for_write(db, user, revision_id)

    patch = data.model_dump(exclude_unset=True)
    if not patch:
        return revision

    try:
        for field, value in patch.items():
            setattr(revision, _PATCH_FIELD_TO_COLUMN[field], value)
        await db.commit()
    except IntegrityError as exc:
        # A patched FK-backed field pointed at a non-existent target. Roll the whole
        # patch back (nothing partially applied) and report a stable 4xx.
        await db.rollback()
        raise InvalidReference() from exc
    except Exception:
        await db.rollback()
        raise
    await db.refresh(revision)
    return revision


async def soft_delete_revision(
    db: AsyncSession, user: UserDB, revision_id: int
) -> BibleRevision:
    """Soft-delete a revision (owner or admin only). Mirrors v3 ``DELETE /revision``.

    Authorized by the shared :func:`_get_revision_for_write` gate — 404 for a truly
    absent id, 403 for a caller who is neither owner nor admin. Idempotent: re-deleting
    an already-soft-deleted row is allowed. The verse rows are left in place, exactly as
    v3 leaves them; this flips a flag, it does not reclaim storage.
    """
    revision = await _get_revision_for_write(db, user, revision_id)

    try:
        revision.deleted = True
        # date.today() rather than a full timestamp: it is what both v3's delete and
        # version_service.soft_delete_version write, the column is not on the wire, and
        # a naive TIMESTAMP column rejects an aware datetime anyway (see
        # utils.datetime_utils.as_naive_utc). Not worth an unexplained difference
        # between the two v4 services.
        revision.deletedAt = date.today()
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    return revision


async def versions_for_revisions(
    db: AsyncSession, revisions: list[BibleRevision]
) -> dict[int, BibleVersion]:
    """Map version id -> ``BibleVersion`` for the parents of ``revisions``.

    Batch-loaded in one query (avoids N+1) and shared by list/get/create/patch so the
    denormalized ``version_abbreviation`` / ``iso_language`` on every ``RevisionOut`` is
    produced the same way. Keyed by *version* id, which is what the response builder
    looks up (``version_map.get(revision.bible_version_id)``).
    """
    version_ids = {r.bible_version_id for r in revisions if r.bible_version_id}
    if not version_ids:
        return {}

    result = await db.execute(
        select(BibleVersion).where(BibleVersion.id.in_(version_ids))
    )
    return {version.id: version for version in result.scalars().all()}
