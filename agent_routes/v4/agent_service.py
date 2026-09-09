"""Data access for the v4 agent-result reads (issue #896, epic #842).

Two reads over the two tables an ``agent-critique`` run writes:

* :func:`get_critique_issues` — ``agent_critique_issue``, the problems the agent found.
* :func:`get_translations` — ``agent_translations``, the text it produced and the
  back-translations explaining it.

:mod:`agent_routes.v4.agent_routes` owns the HTTP half; this module owns queries and
nothing else. Authorization is **not** written here, which is the single most important
thing about the module.


Authorization is borrowed whole, not re-derived
-----------------------------------------------

Both reads start with :func:`assessment_routes.v4.assessment_service.get_assessment`
called with ``types=AGENT_CRITIQUE_ASSESSMENT_TYPES``. That is the assessment family's
one visibility predicate, and it resolves every reason a caller cannot have the resource
— no such id, outside your groups, soft-deleted, a training row, the wrong type — into a
single :class:`~assessment_routes.v4.assessment_service.AssessmentNotFound`. Four of the
assessments slice's five security issues came from authorization written per endpoint, so
a second predicate here would be the same mistake with a new table.

Importing it across packages is deliberate. These reads hang off ``/v4/assessments/{id}``
and are governed by an assessment's visibility, so the predicate belongs to that slice;
copying the query would create a second place for the rules to drift, and re-deriving the
type gate as a check on the loaded row (rather than a clause on the same statement) would
make "wrong type" distinguishable from "not yours" from the outside.


Ordering, and what each read has to join to get it
--------------------------------------------------

Both read in canonical Bible order, which neither v3 endpoint does — and for the issues
read that is a real fix rather than a nicety. v3's ``GET /agent/critique`` orders by
``AgentCritiqueIssue.book``, a **text** column, so its books come back alphabetically:
``ACT`` before ``GEN``, ``EXO`` after ``EPH``. The other v4 typed reads all retired the
same class of bug, and the fix is the same one: join ``book_reference`` and sort on its
``number``.

The two tables need different amounts of help to get there, which is the one structural
difference between the two queries:

* ``agent_critique_issue`` stores the **whole** location triple (``book``, ``chapter``,
  ``verse``) beside its ``vref``, so :func:`_placed_critique_issues` joins
  ``book_reference`` for exactly one thing — the book ordinal — and filters the scope
  against the stored columns, which ``ix_agent_critique_issue_book_chapter_verse``
  covers.
* ``agent_translations`` stores **only** ``vref``, like ``text_lengths_table``, so
  :func:`_placed_translations` walks the same three reference tables
  ``_placed_text_lengths`` does to recover the triple it filters and sorts on.

**Both joins are inner, and each read's ``total`` is counted over the joined set**, so
the count can never promise rows the page cannot show. On the issues read the join can
in principle drop a row: ``book`` is ``String(10)`` with no foreign key, so a value that
names no book is possible and is excluded from the page and the total together. That is
the discipline ``_placed_text_lengths`` documents, and a statement-shape test pins the
join kind because no fixture in the suite makes inner and outer behave differently.

The issues read does **not** join ``verse_reference``, and that is a choice rather than
an omission: it would drop any row whose triple is not canonical, which is stricter than
the ordering needs on a table that stores the triple itself. Only the book ordinal has to
come from a reference table.


Volumes, and why pagination here is for consistency rather than load
--------------------------------------------------------------------

``agent-critique`` is capped at **one chapter** per run by the runner repository, not by
this API, and ``ASSESSMENT-STORAGE-ANALYSIS.md`` bears that out: 12,886
``agent_critique_issue`` rows and 13,559 ``agent_translations`` rows across 1,001
assessments — about 13 of each per run — with the **median** agent-critique assessment
occupying 452 bytes and the 90th percentile 67 KB. So the typical whole result set is
smaller than one default page.

Both reads still take the standard envelope and the family's
:class:`~api_v4.pagination.ResultPaginationParams` bounds (100 default, 1000 maximum). No
new pagination class: a slice whose result sets are this small has no case for its own
limits, and the envelope is what makes these reads substitutable for the other eight in a
client's result-fetching code.
"""

__version__ = "v4"

from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.schemas.assessment import VerseScope
from assessment_routes.v4.assessment_service import get_assessment
from bible_routes.v4 import verse_range_service
from database.models import (
    AgentCritiqueIssue,
    AgentTranslation,
    BookReference,
    ChapterReference,
    UserDB,
    VerseReference,
)
from schemas.assessment import AssessmentType

#: The only assessment type that writes either agent-result table, and so the only type
#: these two reads serve. A tuple of one for the same reason
#: ``assessment_service.NGRAMS_ASSESSMENT_TYPES`` is one, and taken from the enum rather
#: than written as a literal so a renamed value fails at import instead of silently
#: narrowing both reads to nothing.
AGENT_CRITIQUE_ASSESSMENT_TYPES = (AssessmentType.agent_critique.value,)


def _placed_critique_issues(
    assessment_id: int,
    scope: VerseScope,
    *,
    dimension: str | None,
    subtype: str | None,
    min_severity: int | None,
    resolved: bool | None,
    agent_translation_id: int | None,
):
    """The assessment's issues as a subquery, filtered, carrying the book ordinal.

    Every filter is an equality or a bound on a stored, indexed column, and each is
    applied only when the caller sent it — an absent filter adds no clause rather than a
    tautological one, so the planner sees the narrowest predicate the request implies.

    **``min_severity`` excludes null-severity rows, and that is v3's behaviour kept
    rather than a decision retaken.** ``severity >= n`` is *unknown* for a null in SQL's
    three-valued logic, so a row on which the agent omitted a severity is filtered out.
    That is defensible — a client asking for "3 and above" is asking about judged
    severity — but it is the one filter here that hides rows for a reason the parameter
    name does not state, so the field and the parameter both say so. Not coercing the
    null to 0 is the other half of the same decision: the column records that the agent
    declined to judge, which is a different fact from a low judgement.

    ``dimension`` and ``subtype`` are exact, case-sensitive matches against the stored
    values, which is what makes ``?dimension=`` round-trip against what the read emits.
    See :mod:`api_v4.schemas.agent` for why the stored spelling is not normalized on the
    way out — and therefore must not be normalized on the way in either.

    No ``vref`` filter, deliberately, though v3 has one: ``?book=JHN&chapter=1&verse=1``
    already names a single verse, so a ``vref`` parameter would be a second spelling of
    a filter this endpoint already has. Two names for one concept is what guide §10's
    path rule exists to prevent, and the triple is the spelling the rest of the family
    uses. It is also the spelling the one known client already sends: it puts ``book``,
    ``chapter`` **and** ``verse`` on its ``GET /agent/critique`` call today, and v3
    declares only ``book``, so FastAPI has been discarding the other two all along.
    """
    clauses = [AgentCritiqueIssue.assessment_id == assessment_id]
    if scope.book is not None:
        clauses.append(AgentCritiqueIssue.book == scope.book)
    if scope.chapter is not None:
        clauses.append(AgentCritiqueIssue.chapter == scope.chapter)
    if scope.verse is not None:
        clauses.append(AgentCritiqueIssue.verse == scope.verse)
    if dimension is not None:
        clauses.append(AgentCritiqueIssue.dimension == dimension)
    if subtype is not None:
        clauses.append(AgentCritiqueIssue.subtype == subtype)
    if min_severity is not None:
        clauses.append(AgentCritiqueIssue.severity >= min_severity)
    if resolved is not None:
        clauses.append(AgentCritiqueIssue.is_resolved == resolved)
    if agent_translation_id is not None:
        clauses.append(AgentCritiqueIssue.agent_translation_id == agent_translation_id)

    return (
        select(
            AgentCritiqueIssue.id,
            AgentCritiqueIssue.assessment_id,
            AgentCritiqueIssue.agent_translation_id,
            AgentCritiqueIssue.vref,
            AgentCritiqueIssue.book,
            AgentCritiqueIssue.chapter,
            AgentCritiqueIssue.verse,
            AgentCritiqueIssue.dimension,
            AgentCritiqueIssue.subtype,
            AgentCritiqueIssue.detector,
            AgentCritiqueIssue.source_text,
            AgentCritiqueIssue.draft_text,
            AgentCritiqueIssue.comments,
            AgentCritiqueIssue.severity,
            AgentCritiqueIssue.evidence,
            AgentCritiqueIssue.suggestions,
            AgentCritiqueIssue.is_resolved,
            AgentCritiqueIssue.resolved_by_id,
            AgentCritiqueIssue.resolved_at,
            AgentCritiqueIssue.resolution_notes,
            AgentCritiqueIssue.created_at,
            BookReference.number.label("book_number"),
        )
        .join(BookReference, BookReference.abbreviation == AgentCritiqueIssue.book)
        .where(*clauses)
        .subquery()
    )


async def get_critique_issues(
    db: AsyncSession,
    user: UserDB,
    assessment_id: int,
    *,
    scope: VerseScope,
    dimension: str | None = None,
    subtype: str | None = None,
    min_severity: int | None = None,
    resolved: bool | None = None,
    agent_translation_id: int | None = None,
    limit: int,
    offset: int,
) -> tuple[list, int, dict[tuple[str, int, int], list[str]]]:
    """One page of an assessment's critique issues, the total, and the span map.

    Authorized by :func:`get_assessment` with ``types=AGENT_CRITIQUE_ASSESSMENT_TYPES``,
    so an assessment of a type this read does not serve is refused by the same clause as
    one the caller cannot see. See the module docstring for why none of that is written
    here.

    **Ordered by book, chapter, verse, then severity descending with nulls last, then
    row id.** The first three are canonical Bible order and fix v3's alphabetical books;
    the severity leg is v3's and is kept because it is genuinely useful — the worst
    problem in a verse is the one to read first — and its ``nulls_last`` is load-bearing,
    since PostgreSQL puts nulls *first* under ``DESC`` by default and would otherwise open
    every verse with the issues the agent declined to grade. The trailing ``id`` is what
    v3 lacks: without a total order two rows tying on all four keys can swap between
    pages, so ``offset`` pagination is only stable with it.

    No watermark is returned, and the reason is stronger here than on the sibling reads.
    ``/results`` and ``/text-lengths`` omit ``next_updated_since`` because their tables
    carry no modification timestamp. This table carries none *and* has a write path:
    ``PATCH …/critique-issues/{issue_id}`` mutates ``is_resolved``, ``resolved_by_id``,
    ``resolved_at`` and ``resolution_notes`` without touching ``created_at``. So a delta
    feed keyed on ``created_at`` would not merely be unavailable — it would look like it
    worked while silently missing every resolution. ``resolved_at`` cannot stand in
    either: unresolving sets it back to null, and it says nothing about creation.
    """
    assessment = await get_assessment(
        db, user, assessment_id, types=AGENT_CRITIQUE_ASSESSMENT_TYPES
    )
    placed = _placed_critique_issues(
        assessment_id,
        scope,
        dimension=dimension,
        subtype=subtype,
        min_severity=min_severity,
        resolved=resolved,
        agent_translation_id=agent_translation_id,
    )
    total = await db.scalar(select(func.count()).select_from(placed))
    rows = (
        await db.execute(
            select(placed)
            .order_by(
                placed.c.book_number,
                placed.c.chapter,
                placed.c.verse,
                desc(placed.c.severity).nulls_last(),
                placed.c.id,
            )
            .limit(limit)
            .offset(offset)
        )
    ).all()
    continuations = await verse_range_service.continuations_for_revision(
        db, assessment.revision_id
    )
    return list(rows), total or 0, continuations


def _placed_translations(assessment_id: int, scope: VerseScope):
    """The assessment's translations as a subquery, filtered, carrying the location triple.

    The join chain is :func:`assessment_routes.v4.assessment_service._placed_text_lengths`'
    chain, for the same reason: this table stores only ``vref``, so the triple the scope
    filters and the canonical sort need has to come from the reference tables::

        agent_translations.vref  ->  verse_reference.full_verse_id
        verse_reference.book_reference -> book_reference.abbreviation -> .number
        verse_reference.chapter  ->  chapter_reference.full_chapter_id -> .number
        verse_reference.number                                           = the verse

    **No ``DISTINCT ON``, where ``_placed_text_lengths`` has one.** There the natural key
    is ``vref`` and two rows for one verse can only be a retried push, so the read keeps
    the first. Here more than one row per vref is legitimate — it is a second *attempt* at
    the verse, carrying its own text — so collapsing them would be discarding data the
    ordinal exists to distinguish. That is also why ``attempt`` joins the sort key rather
    than a dedup clause.

    **No ``first_vref`` / ``last_vref``, which v3 has and the one known client sends.**
    They become nearly unreachable once the subject is an explicit assessment id, the same
    way ``/alignment-scores`` lost v3's alternative resolvers: an ``agent-critique`` run
    is capped at one chapter, so the widest set this path can return is one chapter of
    verses — about 13 rows at the measured median, 176 at the canon's longest chapter —
    against a default page of 100. A vref *range* filter over that is a convenience with
    no request it makes possible, and ``?book=`` / ``?chapter=`` already narrow it.
    """
    clauses = [AgentTranslation.assessment_id == assessment_id]
    if scope.book is not None:
        clauses.append(VerseReference.book_reference == scope.book)
    if scope.chapter is not None:
        clauses.append(ChapterReference.number == scope.chapter)
    if scope.verse is not None:
        clauses.append(VerseReference.number == scope.verse)

    return (
        select(
            AgentTranslation.id,
            AgentTranslation.assessment_id,
            AgentTranslation.revision_id,
            AgentTranslation.reference_version_id,
            AgentTranslation.script,
            AgentTranslation.vref,
            AgentTranslation.version,
            AgentTranslation.draft_text,
            AgentTranslation.hyper_literal_translation,
            AgentTranslation.literal_translation,
            AgentTranslation.english_translation,
            AgentTranslation.alternatives,
            AgentTranslation.created_at,
            BookReference.number.label("book_number"),
            VerseReference.book_reference.label("book"),
            ChapterReference.number.label("chapter"),
            VerseReference.number.label("verse"),
        )
        .join(VerseReference, VerseReference.full_verse_id == AgentTranslation.vref)
        .join(
            ChapterReference,
            ChapterReference.full_chapter_id == VerseReference.chapter,
        )
        .join(
            BookReference,
            BookReference.abbreviation == VerseReference.book_reference,
        )
        .where(*clauses)
        .subquery()
    )


async def get_translations(
    db: AsyncSession,
    user: UserDB,
    assessment_id: int,
    *,
    scope: VerseScope,
    limit: int,
    offset: int,
) -> tuple[list, int, dict[tuple[str, int, int], list[str]]]:
    """One page of an assessment's agent translations, the total, and the span map.

    Authorized exactly as :func:`get_critique_issues` is, by the same call with the same
    type tuple — the two reads are indistinguishable from the outside on every refusal,
    which is the property the shared predicate exists to give.

    **Ordered by book, chapter, verse, then attempt ascending, then row id.** Canonical
    Bible order first, so the page reads as the text does; ``attempt`` ascending within a
    verse so successive attempts at one verse arrive in the order they were made rather
    than interleaved by id. ``id`` closes the order, for the same offset-stability reason
    the issues read gives.

    The ``verse_reference`` join must stay inner. ``agent_translations.vref`` is non-null,
    so unlike ``text_lengths_table`` there is no null row for it to drop — but a ``vref``
    that is not a canonical verse would still be unplaceable, and because both the page
    and the ``COUNT`` are built from this one subquery, such a row is excluded from both
    together. An ``outerjoin`` would make ``total`` count rows no page can show and would
    then emit nulls for a ``chapter`` and ``verse`` that are required on the response
    model.

    No watermark, for the reason :func:`get_critique_issues` gives — minus the write path,
    since nothing in v4 mutates this table.
    """
    assessment = await get_assessment(
        db, user, assessment_id, types=AGENT_CRITIQUE_ASSESSMENT_TYPES
    )
    placed = _placed_translations(assessment_id, scope)
    total = await db.scalar(select(func.count()).select_from(placed))
    rows = (
        await db.execute(
            select(placed)
            .order_by(
                placed.c.book_number,
                placed.c.chapter,
                placed.c.verse,
                placed.c.version,
                placed.c.id,
            )
            .limit(limit)
            .offset(offset)
        )
    ).all()
    continuations = await verse_range_service.continuations_for_revision(
        db, assessment.revision_id
    )
    return list(rows), total or 0, continuations


class CritiqueIssueNotFound(Exception):
    """No such critique issue **on the assessment named in the path**.

    Separate from :class:`~assessment_routes.v4.assessment_service.AssessmentNotFound`
    because the two name different resources and the caller needs to know which of the
    two path segments was wrong — the parent is refused before the child is looked up, so
    the signals cannot be conflated.

    It deliberately covers two cases: no row with that id anywhere, and a row that exists
    but belongs to a different assessment. The lookup is one statement scoped to the path
    assessment, so those are not distinguishable from the outside, and that is the point
    — otherwise a caller who can read one assessment could probe which issue ids exist on
    assessments they cannot.
    """

    def __init__(self, issue_id: int) -> None:
        self.issue_id = issue_id
        super().__init__(f"Critique issue {issue_id} not found on this assessment")


async def resolve_critique_issue(
    db: AsyncSession,
    user: UserDB,
    assessment_id: int,
    issue_id: int,
    *,
    resolved: bool,
    resolution_notes: str | None,
) -> tuple[AgentCritiqueIssue, dict[tuple[str, int, int], list[str]]]:
    """Assert an issue's resolution; return the row as it now stands, and the span map.

    The slice's only write, and the one endpoint replacing v3's ``/resolve`` +
    ``/unresolve`` pair.

    **Authorized by read access, not ownership**, which is the one place this write
    departs from the rest of the v4 write surface. Every other v4 write goes through an
    owner-or-admin gate; this one calls the same
    :func:`~assessment_routes.v4.assessment_service.get_assessment` predicate the two
    reads call, with the same type tuple. Two reasons. Resolving a critique issue is
    shared review work — a team works through a translation's issues together, and
    restricting it to whoever submitted the run would leave a reviewer able to read an
    issue and unable to act on it. And the row itself is built for that: it carries a
    ``resolved_by_id`` at all *because* more than one person can resolve, which would be
    a pointless column under an owner-only gate. It is also v3's rule, which authorizes
    this write with exactly the read predicate.

    The visible consequence is that this write answers **no 403**: a caller who cannot
    reach the assessment gets the family's 404, and everyone who can reach it may write.
    So it is a write that does not appear in ``V4_FORBIDDEN_RESPONSE``'s set, and
    ``TestForbiddenIsWriteOnly`` should keep it out.

    **The body is the resolution being asserted, and this writes exactly it.**
    ``resolved=True`` sets the flag, stamps ``resolved_by_id`` from the authenticated
    caller and ``resolved_at`` from the database clock, and sets the notes to what was
    passed — ``None`` included, so omitting notes on an issue that has them clears them.
    ``resolved=False`` clears all four together, which is v3's behaviour and keeps the
    notes describing the resolution currently in force rather than a past one.

    **A request asserting what is already stored writes nothing.** Not merely "commits
    no change" — it issues no ``UPDATE``, so ``resolved_at`` does not move and a retried
    request cannot silently re-date a resolution. That is what makes this idempotent in
    the sense a client needs after a dropped response, and it is the same treatment
    :func:`bible_routes.v4.version_service.update_version` gives an empty patch. v3
    instead answers **400** for both re-assertions, which is the wrong answer for a
    ``PATCH``: the state the client asked for is the state that exists.

    Note "already stored" includes *who* stored it. A different user asserting the same
    resolution with the same notes **does** write, taking over ``resolved_by_id`` and
    ``resolved_at`` — because the row's job is to say who currently stands behind the
    resolution, and after that request it is them.

    The span map comes back alongside the row because the response is the *same* shape
    the collection read returns, ``vrefs`` included — so the handler needs it to build a
    row at all. Fetching it here rather than in the router keeps every database read in
    this layer, and it is memoised per revision, so the write pays nothing the read has
    not already paid.

    **A soft-deleted parent makes an issue permanently unresolvable, including for an
    admin**, and that is a deliberate narrowing rather than an oversight. The sibling
    write gate
    :func:`assessment_routes.v4.assessment_service._get_assessment_for_write` passes
    ``include_deleted=True`` precisely so a deleted row stays writable — a delete has to
    be idempotent, and a row whose revision was deleted still has to be deletable. That
    argument does not transfer: resolving an issue on a run nobody can read accomplishes
    nothing, and widening the gate here would let a caller write a resolution and then be
    unable to read it back, since both reads answer 404 on the same row. So this write
    uses the plain read predicate and refuses exactly what the reads refuse. Note the
    filter cascades — soft-deleting a *version* or *revision* hides its assessments too —
    but it is recoverable: undeleting restores resolution. Raised in review of #944.

    Raises :class:`~assessment_routes.v4.assessment_service.AssessmentNotFound` for an
    unreachable parent, and :class:`CritiqueIssueNotFound` for an issue that is not on
    it.
    """
    assessment = await get_assessment(
        db, user, assessment_id, types=AGENT_CRITIQUE_ASSESSMENT_TYPES
    )
    issue = (
        (
            await db.execute(
                select(AgentCritiqueIssue).where(
                    AgentCritiqueIssue.id == issue_id,
                    AgentCritiqueIssue.assessment_id == assessment_id,
                )
            )
        )
        .scalars()
        .first()
    )
    if issue is None:
        raise CritiqueIssueNotFound(issue_id)

    # After the issue is found, not before: a request naming an issue that is not on
    # this assessment needs no span map to be refused, and on a cold memo this is one
    # or two statements. (Copilot review, PR #944.)
    continuations = await verse_range_service.continuations_for_revision(
        db, assessment.revision_id
    )

    if resolved:
        # ``resolved_at is not None`` belongs in this predicate even though the
        # endpoint always stamps it, because a row written outside this endpoint can
        # hold ``is_resolved=True`` with a null timestamp. Without the clause such a
        # row would be read as "already stored" and never repaired, leaving it
        # permanently resolved-but-unstamped and contradicting the contract that
        # ``resolved: true`` records the resolver *now*. With it, a properly stamped
        # row still no-ops, so idempotency is unaffected. (Copilot review, PR #944.)
        already = (
            issue.is_resolved
            and issue.resolved_at is not None
            and issue.resolved_by_id == user.id
            and issue.resolution_notes == resolution_notes
        )
        if already:
            return issue, continuations
        issue.is_resolved = True
        issue.resolved_by_id = user.id
        issue.resolved_at = func.now()
        issue.resolution_notes = resolution_notes
    else:
        already = (
            not issue.is_resolved
            and issue.resolved_by_id is None
            and issue.resolved_at is None
            and issue.resolution_notes is None
        )
        if already:
            return issue, continuations
        issue.is_resolved = False
        issue.resolved_by_id = None
        issue.resolved_at = None
        issue.resolution_notes = None

    # Guarded exactly as the sibling v4 writes are (``version_service.update_version``,
    # ``revision_service``, ``assessment_service.soft_delete_assessment``): a failing
    # commit must leave the session usable rather than in a failed transaction until
    # request teardown, and the pending attribute changes must not survive it. There is
    # no domain error to translate here — no FK this write can point at a missing row —
    # so the exception re-raises to the #828 catch-all as the 500 it is.
    try:
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    await db.refresh(issue)
    return issue, continuations
