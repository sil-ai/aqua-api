"""v4 agent-result schemas (issue #896, epic #842).

The row shapes for the two reads that close the one place v4 was *broken* rather than
merely incomplete: v4 can already start an ``agent-critique`` run
(``POST /v4/assessments {"options": {"type": "agent-critique", ...}}``) and, until these
landed, had nowhere to read the result.

* :class:`CritiqueIssueOut` — one row of ``GET /v4/assessments/{id}/critique-issues``.
* :class:`AgentTranslationOut` — one row of ``GET /v4/assessments/{id}/translations``.

Its own module rather than more classes on :mod:`api_v4.schemas.assessment`, which is
already ~2,250 lines. The split follows the *family*, not the path: guide §15.7 rules
that critique issues and agent translations are assessment results and so hang off
``/v4/assessments/{id}/…``, while lexeme cards and agent word alignments are
version-keyed reference data that promote to their own top-level collections. Those are
the same family and will want a home beside these two, so the module is named for the
family rather than for today's parent path.

Three naming decisions here are worth reading before changing a field
---------------------------------------------------------------------

**``is_resolved`` becomes ``resolved``.** Guide §10's boolean rule: a boolean is bare,
and ``is_admin`` / ``is_reference`` are grandfathered as the **only** two. Nothing else
on the v4 surface keeps the prefix, so neither does this.

**``agent_translations.version`` becomes ``attempt``.** The column is the translation's
own attempt ordinal (``Integer``, default 1), *not* a Bible version — and this codebase
uses "version" for a ``bible_version`` everywhere else, including
``reference_version_id`` on the same table. A wire field called ``version`` here would be
actively misleading, and ``revision_number`` is no better because "revision" also means a
Bible revision. ``attempt`` is bare and says what the number counts.

It is also the honest name across **both** v3 writers, which assign it differently — a
v3 inconsistency this read inherits and cannot fix:

* ``POST /agent/translation`` (singular) takes ``max(version)`` over
  ``(revision_id, reference_version_id, script, vref)``, so its ordinal is per-verse.
* ``POST /agent/translations`` (bulk) takes ``max(version)`` over
  ``(revision_id, reference_version_id, script)`` only, so every verse in one push shares
  one number.

So the value is "the nth stored attempt at this text" under one writer and "the nth push
for this text" under the other. ``attempt`` is true of both; ``batch`` or ``run`` would be
true of only one. See :attr:`AgentTranslationOut.attempt`, which says so on the wire.

**``script`` becomes ``iso_script``.** The column is ``script`` but its foreign key is
``iso_script.iso15924``, and v4 already spells that concept ``iso_script`` on versions
(``VersionOut.iso_script``). One concept, one name across the surface.

And one that deliberately departs from the canon
------------------------------------------------

**``dimension`` keeps its stored spelling, including ``linguistic_conventions``.** Guide
§10's enum rule wants lowercase-hyphenated, which would make that value
``linguistic-conventions``. It is not applied here, and the reason is the same one that
makes ``JobState`` the *opposite* call. ``JobState`` is a closed public vocabulary that v4
defines and translates at the edge from the internal spelling, so v4 owns both sides of
the mapping. ``dimension`` is a ``String(50)`` column with no database constraint, written
by the v3 runner push and by nothing in v4. Hyphenating it on the way out would need a
total, invertible mapping so that ``?dimension=`` still round-trips — and a legacy or
unrecognised stored value has no image under such a mapping, so it would come back
either unfiltered or unrepresentable. Passing the stored value through is the honest
option: the client filters on exactly what it reads back.

Why the JSONB columns *are* typed here, where assessment ``options`` is not
--------------------------------------------------------------------------

``evidence`` (``list[str]``), ``suggestions`` and ``alternatives``
(``list[{text, note?}]``) are JSONB, so typing them risks a 500 on a stored row that does
not match the shape. :attr:`~api_v4.schemas.assessment.AssessmentOut.options` faced the
same choice and deliberately stayed an open object — but for a reason that does not
transfer. ``options`` has **no** enforced shape anywhere: v3 accepts an open dictionary
and a row created through v3 may carry keys no v4 union member declares, so typing it
would 500 on rows that exist today.

These three have an enforced shape and have had since #811: v3's own ``CritiqueIssueOut``
and ``AgentTranslationOut`` already declare them as ``Optional[List[str]]`` and
``Optional[List[SuggestionItem]]``, and the only writers are the runner pushes, which
build them through ``sanitize_suggestion_items`` into exactly ``{"text": ..., "note":
...}``. So typing them here is *matching* v3 rather than diverging from it, and a row
shaped badly enough to break this read already breaks the v3 read it was written through.

Where v4 does diverge, it diverges looser: :class:`SuggestedTextOut` drops v3's
``min_length=1`` on ``text``. ``sanitize_text`` strips control characters *after*
``SuggestionItem`` has validated the input, so a suggestion posted as ``"\\x00"`` passes
v3's ``min_length=1`` and is stored as ``""`` — which v3 then 500s on when reading its
own row back. Dropping the bound is the smaller contract: v4 serves the empty string it
was given instead of refusing the whole page over one field. Note this is *not* guide
§10's "absent text is null" rule being broken — that rule governs what v4 writes for a
missing value, and this is v4 reporting a value v3 stored.
"""

from datetime import datetime

from pydantic import Field

from api_v4.schemas.base import V4BaseModel

#: Bounds for the ``min_severity`` query parameter on ``GET …/critique-issues``, matching
#: v3's documented 1..5 range and ``IssueIn.severity``'s own ``ge``/``le``.
#:
#: Deliberately **not** applied to :attr:`CritiqueIssueOut.severity`. The column is a
#: plain ``Integer`` with no check constraint, so a row stored outside ``IssueIn`` can
#: hold 0 or 9; bounding the response field would turn such a row into a 500 on a read
#: that could otherwise report it. v3 makes the same split for the same reason — its
#: ``IssueIn`` carries the bounds and its ``CritiqueIssueOut`` does not.
MIN_SEVERITY = 1
MAX_SEVERITY = 5


class SuggestedTextOut(V4BaseModel):
    """A proposed piece of text with an optional note.

    One model for two fields, following v3's single ``SuggestionItem``: it is
    :attr:`CritiqueIssueOut.suggestions`, where the text replaces a **span** the agent
    objected to, and :attr:`AgentTranslationOut.alternatives`, where it is another
    rendering of a **whole verse**. The shape is identical and the writers build both
    through the same helper, so a second identical schema would only give
    ``/v4/openapi.json`` two names for one object. The name says what the object *is*
    rather than which of the two uses it is serving.
    """

    text: str = Field(
        description=(
            "The proposed text. Can be an empty string on a row whose original input "
            "was nothing but control characters — the v3 writer sanitizes after "
            'validating, so it stores `""` for input its own schema accepted. Served '
            "as stored rather than refused; see the module docstring."
        ),
    )
    note: str | None = Field(
        default=None,
        description="Why the agent proposed it, or null when it gave no reason.",
    )


class CritiqueIssueOut(V4BaseModel):
    """One row of ``GET /v4/assessments/{id}/critique-issues``.

    **A row is one problem the agent found in one verse**, classified against MQM. A verse
    contributes as many rows as it has issues, and a verse the agent was happy with
    contributes none — so this is not a per-verse result set and a missing verse means
    "nothing flagged", not "not assessed". That is the opposite reading from
    :class:`~api_v4.schemas.assessment.AssessmentResultOut`, where one row per verse is
    the invariant.

    **``vrefs`` says which verses the issue's location covers**, and it is here on the
    same footing as on ``/results`` — a verified fact about this runner, not an
    assumption. ``aqua-assessments/assessments/agent/app.py`` fetches its text with
    ``GET /v3/texts`` at ``include_verses=intersection`` over both revisions, and that
    endpoint runs ``merge_verse_ranges`` **before** filtering, so a revision publishing
    ``MAT 9:20-21`` as one verse yields one merged record whose ``vrefs`` is the whole
    span. The agent therefore sees one verse where the text has one, critiques it once,
    and the continuation gets no row of its own. Without this field a verse missing from
    a result set would be ambiguous between "the agent flagged nothing here" and "this
    verse is part of the span above", and those are very different facts.

    **The map is the assessed revision's, never unioned with the reference's** — the
    correctness argument :func:`assessment_routes.v4.assessment_service.get_results`
    sets out, and it holds here for the same reason: a verse marked ``<range>`` in the
    revision is merged away and so can never also be returned as its own row, which is
    what stops a verse being double-claimed. There is one difference worth knowing, and
    it makes this read's residual case *certain* rather than merely possible. ``/v3/texts``
    checks every requested revision's text for markers, so a span merged only in the
    **reference** is merged too — and the agent always calls it with both revisions. Such
    a verse has no row of its own and is not named by any ``vrefs`` either, so it reads as
    "not critiqued" rather than "covered above". That under-claims; it cannot
    over-claim, and it is what v3 reports today.

    **The full location triple is stored on this table**, unlike ``text_lengths_table``,
    which holds only ``vref`` and has to reach ``verse_reference`` for its triple. So
    ``book``, ``chapter`` and ``verse`` are served from their own columns, and this read
    joins ``book_reference`` for one thing only — the canonical book ordinal it sorts on.

    **``vref`` is rebuilt from that triple rather than served from the stored ``vref``
    column**, which is the same call :class:`~api_v4.schemas.assessment.AssessmentResultOut`
    makes over the same storage shape: where both are stored, the triple is the authority
    and the string is the redundant copy. It matters because v3's push parses the string
    with an unanchored regex, so a stored ``"MAT 9:20-21"`` yields the correct triple
    while keeping its extra characters — and serving that would put a non-verse in a field
    documented as a verse. :func:`agent_routes.v4.agent_routes._to_critique_issue_out`
    has the detail.
    """

    id: int = Field(
        description=(
            "The stored ``agent_critique_issue`` row's id, and the handle "
            "``PATCH /v4/assessments/{id}/critique-issues/{issue_id}`` takes. The only "
            "row id on the typed-result reads that addresses anything."
        ),
    )
    assessment_id: int = Field(
        description="The assessment this issue belongs to (echoed from the path).",
    )
    agent_translation_id: int = Field(
        description=(
            "The translation this issue was raised against — the row on "
            "`GET /v4/assessments/{id}/translations` holding the draft text the agent "
            "objected to. Never null: every issue belongs to exactly one translation, "
            "and deleting that translation cascades to this row. Filter this read by it "
            "to get one verse-attempt's issues on their own."
        ),
    )
    vref: str = Field(
        description=(
            "The verse the issue was found in (`JHN 1:1`) — the **first** verse of the "
            "span where the revision merged several. Formatted from this row's "
            "`book`/`chapter`/`verse`, so it is always a literal canonical vref that "
            "joins against `vref.txt` and can never disagree with the triple beside it "
            "(see the class docstring)."
        ),
    )
    vrefs: list[str] = Field(
        description=(
            "Every verse this row's location covers, in canonical order and beginning "
            "with `vref`. A single entry unless the revision merged verses into this one "
            "(`<range>`), in which case the continuations follow. This is what tells a "
            "verse the agent had nothing to say about apart from a verse that is part of "
            "the span above — the two are otherwise both just absent. See the class "
            "docstring for the one case it under-reports."
        ),
    )
    book: str = Field(
        description="The verse's book, as its USFM abbreviation (`JHN`).",
    )
    chapter: int = Field(description="The verse's chapter number.")
    verse: int = Field(description="The verse's number within the chapter.")
    dimension: str = Field(
        description=(
            "The MQM dimension the issue falls under. The v3 writer constrains this to "
            "`accuracy`, `terminology` or `linguistic_conventions`, and the underscore "
            "in the third is the stored spelling — served as-is rather than hyphenated, "
            "for the reason the module docstring gives. Typed as a string rather than an "
            "enum because the column has no database constraint, so a v4 enum would 500 "
            "on a legacy or hand-written value instead of reporting it."
        ),
    )
    subtype: str = Field(
        description=(
            "The MQM leaf classification, e.g. "
            "`mistranslation/hallucination-numbers`. Free-form as far as this API is "
            "concerned — the vocabulary is the agent's."
        ),
    )
    detector: str | None = Field(
        default=None,
        description=(
            "Which check inside the agent raised the issue, e.g. `number_diff`. Null "
            "when the agent did not say."
        ),
    )
    source_text: str | None = Field(
        default=None,
        description=(
            "The span of the reference text the issue is about, or null. A span, not the "
            "whole verse — the verse's full text is on the translation row this issue "
            "names."
        ),
    )
    draft_text: str | None = Field(
        default=None,
        description=(
            "The span of the draft translation the agent objected to, or null. Compare "
            "with `source_text`; the pair is what the issue is asserting."
        ),
    )
    comments: str | None = Field(
        default=None,
        description="The agent's prose explanation of the problem, or null.",
    )
    severity: int | None = Field(
        default=None,
        description=(
            "How serious the agent judged the issue, 1 to 5. **Null is meaningful and is "
            "not coerced**: it records that the agent omitted a severity, which is a "
            "different fact from a low one. A null-severity row is excluded by "
            "`min_severity` (SQL comparison against null is unknown, and v3 behaves the "
            "same way), so fetch without that filter to see every issue. Unbounded here "
            "even though `min_severity` is bounded — see MIN_SEVERITY."
        ),
    )
    evidence: list[str] | None = Field(
        default=None,
        description=(
            "What the agent offers in support, one string per point, e.g. "
            '`["source: 40", "draft: 14"]`. Null when it offered none; the writer '
            "normalizes an empty list to null, so an empty list does not occur."
        ),
    )
    suggestions: list[SuggestedTextOut] | None = Field(
        default=None,
        description=(
            "Replacements the agent proposes for the objected-to span. Null when it "
            "proposed none — again normalized from an empty list by the writer."
        ),
    )
    resolved: bool = Field(
        description=(
            "Whether someone has marked the issue dealt with, via "
            "`PATCH /v4/assessments/{id}/critique-issues/{issue_id}`. v3's "
            "`is_resolved`, renamed: guide §10 makes a boolean bare, and `is_admin` and "
            "`is_reference` are the only two keeping the prefix. **Never null**, and "
            "required rather than defaulted here because the column is `NOT NULL` in the "
            "database — verified, not inferred from the model — so unlike `flag` and "
            "`hide` on the alignment rows there is no null to coerce."
        ),
    )
    resolved_by_id: int | None = Field(
        default=None,
        description=(
            "Id of the user who resolved it, stamped by the server. Null while "
            "unresolved. It exists because resolving is shared work — anyone who can "
            "read the assessment can resolve its issues, so which of them did it is not "
            "derivable from the assessment's owner."
        ),
    )
    resolved_at: datetime | None = Field(
        default=None,
        description=(
            "When it was resolved, stamped by the server. Null while unresolved. **Not "
            "usable as a delta watermark**: unresolving sets it back to null, and it "
            "says nothing about when the issue itself was created."
        ),
    )
    resolution_notes: str | None = Field(
        default=None,
        description=(
            "What the resolver said about how it was addressed, or null. Cleared when "
            "the issue is unresolved, so it always describes the resolution currently in "
            "force rather than a past one."
        ),
    )
    created_at: datetime | None = Field(
        default=None,
        description=(
            "When the runner stored the issue. Null on a row written before the column "
            "had a default. Note this read publishes **no** `next_updated_since`: the "
            "table has no `updated_at`, and resolving mutates a row without touching "
            "`created_at`, so a delta feed built on this column would silently miss "
            "every resolution."
        ),
    )


class AgentTranslationOut(V4BaseModel):
    """One row of ``GET /v4/assessments/{id}/translations``.

    **A row is one verse as the agent rendered it**, plus the back-translations it
    produced to explain itself. This is the text
    :class:`CritiqueIssueOut.agent_translation_id` points at: an issue says the draft says
    "fourteen days" where the source says "forty", and this row is where that draft lives.

    **Every attempt is returned, and none is hidden.** v3's read collapses to the latest
    ``version`` per vref unless asked for ``all_versions``; v4 returns every stored row
    and labels it with :attr:`attempt`. Two reasons. Under an assessment id the dedup is
    almost always a no-op anyway — the bulk writer gives one push one ordinal, so a run
    that pushed once has exactly one attempt per verse, and it is only v3's *singular*
    writer that can leave two attempts for one verse inside one assessment. And a default
    that drops rows is the thing guide §10's filter rule warns about: a client reading a
    page has no way to learn that older attempts existed. v3's cross-assessment
    latest-per-vref mode is a different matter and is not carried at all — that is the
    same "resolve the latest run for me" resolution v4 declines on ``/score-comparison``.

    **``revision_id``, ``reference_version_id`` and ``iso_script`` are the same on every
    row of a page**, which normally argues for dropping them (emitting a constant per row
    is the mistake v3's phantom ``is_reference`` made). They stay because
    :attr:`attempt` is scoped to exactly that triple and **not** to the assessment: the
    ordinal is unanchored without them, and a client comparing attempts across two
    assessments of the same text needs to see that the triple matches.

    **``vrefs`` matters more on this read than on any other**, because this read is what
    tells a client which verses were critiqued at all. The agent writes a translation row
    for every verse it processed, so the row set *is* the assessed set — and where the
    revision merges ``MAT 9:20-21``, one row under ``MAT 9:20`` holds the whole span's
    draft and the continuation has no row. Without ``vrefs`` the union of this read's
    verses would understate coverage by exactly the merged continuations.
    :class:`CritiqueIssueOut` carries the argument, including the reference-side case
    this under-reports.
    """

    id: int = Field(
        description=(
            "The stored ``agent_translations`` row's id, and the value "
            "`CritiqueIssueOut.agent_translation_id` holds. Not addressable on its own: "
            "no v4 endpoint takes a translation id in its path."
        ),
    )
    assessment_id: int = Field(
        description="The assessment this translation belongs to (echoed from the path).",
    )
    revision_id: int = Field(
        description=(
            "The revision that was translated — the assessment's own `revision_id`, "
            "denormalized onto this table. Part of what `attempt` is counted within."
        ),
    )
    reference_version_id: int = Field(
        description=(
            "The **version** the agent translated from, derived by the writer from the "
            "assessment's reference revision. A version id, not a revision id, which is "
            "why it is not simply the assessment's `reference_id`. Part of what "
            "`attempt` is counted within."
        ),
    )
    iso_script: str = Field(
        description=(
            "The reference version's ISO 15924 script code (`Latn`), four characters. "
            "v3 spells this column `script`; v4 spells the concept `iso_script` here as "
            "it does on versions. Part of what `attempt` is counted within."
        ),
    )
    vref: str = Field(
        description=(
            "The verse this row translates (`JHN 1:1`) — the **first** verse of the span "
            "where the revision merged several, in which case `draft_text` and the "
            "back-translations are the whole span's."
        ),
    )
    vrefs: list[str] = Field(
        description=(
            "Every verse this row covers, in canonical order and beginning with `vref`. "
            "A single entry unless the revision merged verses into this one (`<range>`). "
            "The union of this field across a whole page set is the set of verses the "
            "agent actually processed — which is why subtracting the verses you see from "
            "the range you asked for does not give you the gaps."
        ),
    )
    attempt: int = Field(
        description=(
            "Which attempt at this text the row belongs to, counting from 1 within "
            "`(revision_id, reference_version_id, iso_script)` — **not** within the "
            "assessment. v3's `version`, renamed because it is not a Bible version. "
            "Read it as an ordinal and not as a count: the two v3 writers increment it "
            "over different scopes, so consecutive numbers are not promised and a gap "
            "means another push happened, not that a row is missing. The module "
            "docstring has the detail."
        ),
    )
    draft_text: str | None = Field(
        default=None,
        description=(
            "The agent's translation of the verse, or null where it produced none."
        ),
    )
    hyper_literal_translation: str | None = Field(
        default=None,
        description=(
            "A word-for-word back-translation of `draft_text`, following the draft's own "
            "morphology rather than reading naturally. Null where the agent produced "
            "none."
        ),
    )
    literal_translation: str | None = Field(
        default=None,
        description=(
            "A literal but grammatical back-translation of `draft_text`, or null."
        ),
    )
    english_translation: str | None = Field(
        default=None,
        description=(
            "A natural-English rendering of `draft_text`, so a reviewer who does not "
            "read the target language can follow it. Null where the agent produced none."
        ),
    )
    alternatives: list[SuggestedTextOut] | None = Field(
        default=None,
        description=(
            "Other whole-verse renderings the agent considered, each with the note it "
            "gave. Null when it offered none — the writer normalizes an empty list to "
            "null. Contrast `CritiqueIssueOut.suggestions`, which replaces a span rather "
            "than the verse."
        ),
    )
    created_at: datetime | None = Field(
        default=None,
        description=(
            "When the runner stored the translation. Null on a row written before the "
            "column had a default. As on the issues read, there is no "
            "`next_updated_since`: the table carries no modification timestamp."
        ),
    )
