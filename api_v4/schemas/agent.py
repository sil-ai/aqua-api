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

from pydantic import Field, model_validator

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


class CritiqueIssueResolution(V4BaseModel):
    """Request body for ``PATCH /v4/assessments/{id}/critique-issues/{issue_id}``.

    **One endpoint replaces v3's ``/resolve`` + ``/unresolve`` pair**, which is guide
    §5's rule that the verb belongs to the HTTP method rather than the path. So
    ``resolved`` is required: the body states which resolution you are asserting, and
    there is no default that would let an empty body mean one of them.

    **The body is the resolution you are asserting, and the server writes exactly that.**
    ``resolved: true`` records *you* as the resolver, *now*, with the notes you sent —
    and with no notes at all if you sent none, so a client preserving existing notes
    re-sends them. ``resolved: false`` clears all four fields together. A request that
    asserts precisely what is already stored writes nothing and does not move
    ``resolved_at``, which is what makes a retried request genuinely idempotent rather
    than merely harmless.

    That is the substantive change from v3, which answers **400** for "already resolved"
    and "not currently resolved". Refusing an assertion the row already satisfies is the
    wrong answer for a ``PATCH``: a client whose ``200`` was lost cannot safely retry, and
    the state it wanted is the state that exists. ``PATCH /v4/versions/{id}`` already
    treats a no-op patch as a ``200`` that does not move ``updated_at``; this follows it.

    **Closed allowlist** (``extra="forbid"``), as on every v4 request body. It matters
    more than usual here because three of this table's four resolution columns are
    **not** client-settable: ``resolved_by_id`` and ``resolved_at`` are stamped by the
    server from the authenticated caller and the clock, and a client that could set them
    could attribute a resolution to someone else. They are absent from this model, so
    sending either is a 422 rather than something the handler has to strip.
    """

    resolved: bool = Field(
        description=(
            "The resolution to assert. `true` marks the issue dealt with, recording you "
            "and the current time; `false` reopens it and clears the resolution "
            "entirely. Required — this one endpoint replaces v3's `/resolve` and "
            "`/unresolve` paths, so the body is what distinguishes them."
        ),
    )
    resolution_notes: str | None = Field(
        default=None,
        description=(
            "What you did about it. **Only accepted with `resolved: true`** — sending it "
            "alongside `resolved: false` is a 422, not a silently dropped field, because "
            "a note about how something was resolved has no meaning on an issue being "
            "reopened. Omit it when resolving without a note; omitting it on an issue "
            "that already has notes clears them, since the body is the whole resolution "
            "being asserted."
        ),
    )

    model_config = {
        **V4BaseModel.model_config,
        # Closed for the reason the class docstring gives: `resolved_by_id` and
        # `resolved_at` are server-stamped, and a body that could carry them could
        # attribute a resolution to another user.
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "resolved": True,
                "resolution_notes": "Corrected the number in revision 512.",
            }
        },
    }

    @model_validator(mode="after")
    def _notes_require_resolving(self) -> "CritiqueIssueResolution":
        """Refuse ``resolution_notes`` alongside ``resolved: false``.

        Checked here rather than in the handler so it is a ``422`` in the shared
        validation envelope naming the field, and so ``/v4/openapi.json`` describes one
        model rather than a handler-side rule a reader cannot see. The alternative —
        accepting the notes and dropping them, since unresolving clears them anyway —
        is the silent-ignore failure mode that closed request bodies exist to prevent.
        """
        if self.resolved is False and self.resolution_notes is not None:
            raise ValueError(
                "resolution_notes is only accepted with resolved: true — "
                "unresolving clears the notes"
            )
        return self


class SenseOut(V4BaseModel):
    """One sense of a lexeme — a definition, and the phrases illustrating it.

    **Typed, where v3 serves ``senses`` as a bare ``list``.** The rule the agent-result
    slice settled is to type a JSONB column when its shape is *enforced* somewhere and to
    leave it open when nothing enforces it (assessment ``options`` being the open case).
    This one is enforced three times over in ``aqua-assessments``, which is the only
    writer: a ``Sense(definition: str, examples: List[str])`` Pydantic model, a JSON-schema
    tool contract the card-building model must satisfy, and a defensive normalizer that
    folds looser LLM output into that shape before it is ever sent. The one known reader,
    ``aqua-django-app``, already reads exactly these two keys.

    **``examples`` is a list of plain strings, and is not**
    :attr:`LexemeCardOut.examples`. The card-level field is verse-grounded — each entry
    carries a row id and a source/target pair drawn from a real revision. These are
    illustrative phrases attached to a single definition, with no id and no verse behind
    them. Two different things that v3's untyped ``list`` let share a name.

    In practice the runner writes ``examples: []`` on every canonical card: the only path
    that carries a non-empty list is the translation overlay, which copies the canonical's
    entries forward while replacing the definition. So an empty list here is the norm and
    means "none recorded", not "none exist".
    """

    definition: str = Field(
        description=(
            "What the lexeme means in this sense, in the source-side language. Served as "
            "stored. Can be an empty string on a malformed legacy row — see "
            "`LexemeCardOut.senses` for why such a row is repaired rather than refused."
        ),
    )
    examples: list[str] = Field(
        default_factory=list,
        description=(
            "Phrases illustrating this sense, as plain strings. Usually empty: only the "
            "translation-overlay path carries these forward. Not verse-grounded — for "
            "that, read `LexemeCardOut.examples`."
        ),
    )


class LexemeCardExampleOut(V4BaseModel):
    """One verse-grounded usage example on a lexeme card.

    A row of ``agent_lexeme_card_examples``, filtered to the revisions the caller may
    read — see :class:`LexemeCardOut` for that rule, which is the one piece of
    authorization v3 already applied to this family.
    """

    id: int = Field(
        description=(
            "The stored example's id. Unlike most ids on v4 result rows this one **is** a "
            "handle: it is what `card_translation_examples.example_id` points at, so a "
            "translated example can be matched back to the canonical it translates."
        ),
    )
    revision_id: int = Field(
        description=(
            "The revision the example was drawn from. **v3 does not serve this**, which "
            "left its one client re-sending examples on a `PATCH` with the revision it "
            "happened to be viewing rather than the one each example came from. Always a "
            "revision of the card's source or target version — the write path enforces "
            "that, and the read filter above depends on it."
        ),
    )
    source: str | None = Field(
        default=None,
        description=(
            "The phrase in the source-side language. **Null when a "
            "`source_language_iso` was requested that this card has no translation "
            "for** — the whole source side is null in that case, and "
            "`LexemeCardOut.source_language_iso` is null to say so. Never null on a "
            "canonical read."
        ),
    )
    target: str | None = Field(
        default=None,
        description=(
            "The phrase in the target language. Never null in practice — the column is "
            "`NOT NULL` — and optional here only so that the source-side nulling above "
            "cannot be mistaken for a shape this field shares."
        ),
    )


class LexemeCardOut(V4BaseModel):
    """One row of ``GET /v4/lexeme-cards`` and the whole body of ``GET /v4/lexeme-cards/{id}``.

    **A lexeme card is a dictionary entry for one word of a translation**, built against a
    source text: the target-language lemma, the surface forms it inflects into, what it
    means, and verses where it is used. It is reference data rather than assessment
    output — keyed on a version pair rather than on a run, outliving any single one —
    which is why guide §15.7 promotes it to a top-level collection with no assessment to
    nest under.

    Canonical storage, and the language you asked for
    ------------------------------------------------

    A card is stored once, against the source language it was built in — its
    ``source_language_iso``. The same card can then carry a cheap machine translation of
    its **source side only** into other languages, so a Swahili translator and a Spanish
    translator can read the same card. Pass ``?source_language_iso=`` to pick which:

    * Omitted, or equal to the card's canonical language — the canonical card, verbatim.
    * A language with a stored overlay — :attr:`source_lemma`,
      :attr:`source_surface_forms`, :attr:`senses` and each example's ``source`` come from
      the overlay; everything target-side is untouched, because there is only one target
      column and every language view projects it.
    * **A language with no overlay** — the whole source side is null, including
      :attr:`source_language_iso` itself, and the target side is served as normal. That
      last one is the substantive change from v3, whose by-id read answers **404** in this
      case to signal a derivation pipeline it should trigger. A 404 for a row that plainly
      exists is a side effect wearing a status code, and the pipeline that consumed it
      runs against v3, which is unchanged. So v4 reports the state instead: null
      ``source_language_iso`` means "this card has nothing in the language you asked for".

    v3 published that state as a separate boolean, ``has_translation_overlay``. Guide
    §10's boolean rule is that a boolean is bare and that ``is_admin`` / ``is_reference``
    are a closed pair, so the name could not survive as it was — and once
    ``source_language_iso`` is on the wire, a boolean beside it would be a second field
    saying what the first already says.

    Malformed JSONB is repaired, not refused
    ----------------------------------------

    :attr:`senses`, :attr:`surface_forms` and :attr:`source_surface_forms` are ``jsonb``
    columns with no database constraint, and the only writer that types them arrived after
    rows already existed. A read that validated them strictly would answer **500** on a
    legacy row rather than showing the caller the card — the failure mode the
    ``severity`` note above rejects for the same reason. So the conversion boundary folds
    what it can into the declared shape (a bare string sense becomes a definition with no
    examples) and drops what it cannot, rather than declining to serve the row. This
    mirrors what the runner's own normalizer does before writing.
    """

    id: int = Field(
        description="The card's id, and its handle on `/v4/lexeme-cards/{id}`."
    )
    target_lemma: str = Field(
        description=(
            "The dictionary form of the word in the **target** language — the translation "
            "being worked on. Stored lowercased and NFC-normalized, and that stored "
            "spelling is what is served."
        ),
    )
    source_lemma: str | None = Field(
        default=None,
        description=(
            "The dictionary form in the **source-side** language, i.e. whichever language "
            "`source_language_iso` names. Null when the card never recorded one, and also "
            "null when a `source_language_iso` was requested that this card has no "
            "translation for."
        ),
    )
    source_version_id: int = Field(
        description=(
            "The version the card was built **from**. Often a shared pivot Bible rather "
            "than the reference the caller had in mind, because card lookup is "
            "pivot-routed; that is why it is reported rather than assumed."
        ),
    )
    target_version_id: int = Field(
        description="The version the card is **for** — the translation it describes.",
    )
    source_language_iso: str | None = Field(
        default=None,
        description=(
            "ISO 639-3 code of the language the source-side fields above are actually in. "
            "**Null means the card has no translation into the `source_language_iso` you "
            "asked for**, and the whole source side is null with it. Non-null on every "
            "canonical read: the column is `NOT NULL`, filled by a database trigger from "
            "`source_version_id` when a writer omits it."
        ),
    )
    pos: str | None = Field(
        default=None,
        description=(
            "Part of speech. **Free text, deliberately not an enum.** The card builder "
            "picks from a closed list (`noun`, `verb`, `adjective`, …, `unknown`), but a "
            "second writer — function-word seeding — stores values outside it "
            "(`complementizer`, `demonstrative`, `quantifier`, `tam_marker`). Declaring "
            "the enum would turn those rows into 500s on a read, and the column has no "
            "database constraint to make the enum true."
        ),
    )
    surface_forms: list[str] | None = Field(
        default=None,
        description=(
            "The inflected forms the target lemma appears as in the text. Shared across "
            "every language view — there is one target side, so a correction made from "
            "any view is visible from all of them."
        ),
    )
    source_surface_forms: list[str] | None = Field(
        default=None,
        description=(
            "The inflected forms on the source side. Overlaid per language, and null when "
            "the requested language has no overlay."
        ),
    )
    senses: list[SenseOut] | None = Field(
        default=None,
        description=(
            "What the lemma means, one entry per distinct sense, in the source-side "
            "language. Overlaid per language and null when the requested language has no "
            'overlay. Null also means "none recorded" on a card that never got any.'
        ),
    )
    examples: list[LexemeCardExampleOut] = Field(
        default_factory=list,
        description=(
            "Verse-grounded usages, oldest row first. **Filtered to the revisions you may "
            "read** — a card is visible when you can reach its target version, but an "
            "example is shown only when you can reach the revision it was drawn from, so "
            "two callers can legitimately see the same card with different examples. "
            'Administrators see them all. An empty list therefore means "none you may '
            'read", which is not the same as "none stored".'
        ),
    )
    confidence: float | None = Field(
        default=None,
        description=(
            "How much the builder trusts the card, 0 to 1. Null on rows written before it "
            "was recorded. Cards are returned highest-confidence first, and nulls sort "
            "last."
        ),
    )
    english_lemma: str | None = Field(
        default=None,
        description=(
            "An English gloss, recorded when neither side of the pair is English so that "
            "a card is readable without knowing either language. Null when the source "
            "side is already English, and on rows that never got one."
        ),
    )
    alignment_scores: dict[str, float] | None = Field(
        default=None,
        description=(
            "Source words statistically aligned to this lemma, mapped to a strength, "
            "strongest first. **The values are not probabilities and are not bounded by "
            "1.** Rows from the current eflomal pipeline hold a rate in `(0, 1]` — "
            "alignment links divided by corpus occurrences — while rows from the retired "
            "NLLB pipeline hold an attention score scaled by a frequency factor and can "
            "exceed 1. Nothing on the row says which produced it, so treat the numbers as "
            "a ranking rather than a measurement. Keys are lowercased source words."
        ),
    )
    build_version: str | None = Field(
        default=None,
        description=(
            "Opaque token identifying the build that produced this card, bumped whenever "
            "the builder rebuilds it. **Null on most cards**: only the agentic "
            "card-builder stamps it, while translation-discovered lemmas and function-word "
            "seeds do not. Useful for spotting a translation overlay that has fallen "
            "behind its parent, not as a general version marker."
        ),
    )
    model: str | None = Field(
        default=None,
        description=(
            "The model that built the card, resolved to a foundation-model name rather "
            "than the runtime inference-profile id (e.g. `anthropic.claude-sonnet-4-6`). "
            "Null on the same rows `build_version` is null on. `?model=` filters on it, "
            "and that filter excludes unstamped cards for the same reason."
        ),
    )
    created_at: datetime | None = Field(
        default=None,
        description=(
            "When the card was first stored. Null on legacy rows: the column is nullable "
            "with no database default and its default is applied in Python, so a row "
            "written outside the ORM has none."
        ),
    )
    last_updated: datetime | None = Field(
        default=None,
        description=(
            "When the card was last written, by anyone. Null on the same terms as "
            "`created_at`. **Not a delta watermark** — see the endpoint description for "
            "why this collection publishes no `next_updated_since`."
        ),
    )
    last_user_edit: datetime | None = Field(
        default=None,
        description=(
            "When a human last edited what you are reading, as opposed to the pipeline "
            "rewriting it. Null if nobody ever has. On an overlaid read this is the later "
            "of the canonical row's value and the overlay's, so a source-only edit is "
            'still visible to a client rendering an "edited recently" marker.'
        ),
    )
