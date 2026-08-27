"""v4 Assessment request and read schemas (issues #826/#830/#865/#893, epic #842).

``POST /v4/assessments`` replaces v3's ``AssessmentIn = Depends()`` + loose query
flags + a stringified ``extra_kwargs`` blob with **one JSON body** whose
per-assessment-type options are a **discriminated union on ``type``** (#842's
2026-08-25 decision 1). The win is that ``/v4/openapi.json`` documents exactly what
``word-alignment`` accepts, and anything else is a 422 at the edge instead of a
failure deep inside the Modal runner::

    {
      "revision_id": 12,
      "force": false,
      "options": {"type": "word-alignment", "reference_id": 7, "use_eflomal": false}
    }

Three properties of this module are load-bearing rather than stylistic.

**``reference_id`` lives on the union member, not at the top level.** The three
types that need a reference declare it as a required ``int``
(:class:`ReferencedAssessmentOptions`); the four that do not, do not have the field
at all. This is the single largest usability change in the slice: v3 accepted
``reference_id`` for every type and checked the requirement at runtime (a 400 from
``add_assessment``), while a *missing* one for a type that needs it only failed once
the runner had already been spawned. Here the requirement is in the schema, so it is
in the generated client and in the docs. That the four reference-free types omit the
field entirely is evidence-backed, not tidiness: every result query keyed on
``Assessment.reference_id`` filters ``type == "word-alignment"``, and the
``text-lengths`` comparison pairs two *single-revision* assessments by
``Assessment.revision_id == reference_id`` (``results_query_routes.py:1053-1102``) —
nothing in the codebase reads a reference off a ``tfidf`` / ``ngrams`` /
``text-lengths`` / ``sentence-length`` row.

**Every member is ``extra="forbid"``.** That is what makes "an option belonging to a
different type" a 422 (``use_eflomal`` on ``agent-critique``) rather than a silently
ignored key. The same applies to :class:`AssessmentCreate` itself, which is how the
five inputs v4 deliberately drops — ``extra_kwargs``, ``modal_env``,
``source_version_id``, ``target_version_id``, ``return_all_results`` — announce
themselves to a client porting from v3 instead of being silently discarded. A silent
drop would be actively misleading for ``modal_env``: the caller would believe they
had chosen the execution environment while the server used its own.

**``stored_options()`` is the bridge to the frozen storage shape.** The union is a
wire-format and validation layer over the *same* ``Assessment.kwargs`` JSONB v3
writes, because ``kwargs`` is part of assessment identity on a table v3 still writes
to: create-time dedup compares it exactly
(``assessment_routes/v3/assessment_routes.py:714``) and the read endpoints
discriminate eflomal from fastalign purely on the stored ``use_eflomal`` flag
(``assessment_routes/v3/alignment_filters.py``). So each member states, next to its
own options, exactly what it persists — including v3's deliberate
present-or-absent asymmetry, where a true flag is stored and a false one is stored as
*nothing at all*. Normalizing a false flag to an explicit ``false`` would not error;
it would quietly make v4-created rows invisible to v3's dedup and produce duplicate
GPU runs. ``test_assessment_routes_v4`` pins v3/v4 kwargs parity for exactly this
reason.

``transcribed_audio`` is the one option ``stored_options()`` does **not** resolve: it
can inherit the draft version's own ``transcribed_audio`` column (#815), which needs
a database read. :mod:`assessment_routes.v4.assessment_service` resolves it and
overlays the stored key; see that module for the precedence rules.


The read half: one resource schema, and the poll's merged shape
---------------------------------------------------------------

:class:`AssessmentOut` is the assessment resource, and :class:`AssessmentJob` is that
resource plus the :class:`~api_v4.jobs.JobEnvelope` keys. The split follows what each
endpoint is *for*: the list is a collection of resources, and the poll is a job read.

**The poll returns the row's own fields at every state** (#893's 2026-08-26 decision 1).
A bare envelope would answer a poll on a running assessment with
``{"job_id": "42", "state": "RUNNING", "result": null, "error": null}`` and nothing
else — no type, no revision, no timestamps — because ``result`` is reserved for a
*successful outcome*. That would leave v4 with no way to read one assessment's details
until it finished, which contradicts the epic's "an assessment *is* the job". So
:class:`AssessmentJob` merges the two, and :class:`AssessmentOut` is the shared half
the list serves.

``AssessmentJob`` inherits from ``JobEnvelope`` rather than restating its four keys, so
the envelope's invariants (``error`` iff ``FAILED``; ``result`` only on ``SUCCEEDED``)
are enforced on the merged body by the same validator that guards every other v4 job.
It inherits the **bare** generic, leaving ``result`` untyped — which :mod:`api_v4.jobs`
permits for "a slice whose result shape is genuinely open", and this one is: what a
finished assessment puts in ``result`` (a summary, or a pointer to the typed result
sub-resources) is still an open question on #893, and a ``SUCCEEDED`` job with no
result is explicitly legal. Adding a type there later is additive.

Four things about the wire shape that are decisions rather than accidents:

* **``job_id`` is a string while ``id`` is an integer, for the same row.** Deliberate,
  and documented in :mod:`api_v4.jobs`: the envelope stringifies so a client parses one
  type across assessments, training and predict. In a merged body the two sit side by
  side, so both field descriptions say so.
* **``state`` replaces the internal ``status`` column on the wire, and appears on the
  list too.** v4 translates vocabularies at the edge rather than renaming database
  values, so the lowercase ``queued/running/finished/failed`` spelling never reaches a
  v4 client — one uppercase vocabulary, on the collection as well as the poll.
* **Progress rides along as ordinary fields** (decision 2). ``percent_complete`` and
  ``status_detail`` are populated columns, and :mod:`api_v4.jobs` forbids publishing
  progress through ``result`` ("result is the job's outcome, not its progress"). They
  are plain fields here, so the shared envelope needed no new key and #894/#895
  inherit nothing.
* **``options`` echoes the stored options, and is an open object.** It is what the run
  was created with, so a client can tell an eflomal ``word-alignment`` from a fastalign
  one — a distinction that is part of an assessment's identity. It is *not* the request
  body's ``options``: ``type`` and ``reference_id`` are top-level fields on the way out,
  and a row created through v3 may carry keys no v4 union member declares (``top_k``),
  so typing it as the union would 500 on legacy rows. Open dict, honestly documented.

Three v3 ``AssessmentOut`` fields are deliberately **not** emitted:

* ``status`` — see above; ``state`` is its public spelling.
* ``is_training`` — every row on this surface has it false, because
  ``GET /v4/assessments`` excludes training rows and ``GET /v4/assessments/{id}`` 404s
  on one (decision 3). Emitting a constant false is the same mistake as v3's phantom
  ``is_reference`` on ``RevisionOut``. #895 exposes those rows as their own resource.
* ``attempt_count`` — the timeout sweep's own bookkeeping. The operational verbs left
  the public surface with this slice (#842), and their counter goes with them.


The result reads: two row shapes and one scope, invalid by construction
-----------------------------------------------------------------------

``GET /v4/assessments/{id}/results`` serves the three types whose rows land in
``assessment_result``, and its page holds one of **two** row types rather than one type
with fields nulled:

* :class:`AssessmentResultOut` — a verse-level row, carrying ``vref`` (the span's first
  verse, which is what is stored) *and* ``vrefs`` (every verse it covers).
* :class:`AssessmentResultAggregateOut` — a chapter, book or whole-text rollup, carrying
  only the scope it summarizes and the rolled-up score and flags.

Why two types and not one. v3 projects a *different column set* when aggregating: it
drops ``vref``, ``source``, ``target`` and ``note``, and at ``aggregate=text`` there are
no group columns at all, so the single row has no location of any kind. Modelling that as
the verse row with four fields nulled would put ``vrefs`` on a row covering a whole book,
where it is either meaningless or 30,000 entries long — #893's 2026-08-27 decision 5.
Two types make ``vrefs`` structurally absent under aggregation instead of conventionally
empty, and a client branches on ``aggregate`` in its *request* to know which it gets.

``source`` and ``target`` are on neither type. They are populated only for
missing-words-shaped assessments, whose per-word rows this read does not serve, and v3's
``/result`` never returns them either (its grouped projection drops them for every type).
Serving them here would be a new capability rather than a port, so they wait for the
read that actually needs them.

:class:`ResultScope` is the request half, and it is where #486's principle is paid off.
v3 guards the same four parameters with ``validate_parameters``, a runtime function that
raises five separate ``HTTPException(400)``s and is frozen with the rest of v3. Here the
invariants are a ``model_validator``, so an inconsistent scope **cannot be constructed**:
the service layer never has to re-check, the rules are unit-testable without an HTTP
client, and the failure is the standard 422 envelope rather than a bespoke 400. The
adapter that turns query parameters into one of these lives with the route, so OpenAPI
still documents four flat query parameters rather than one object.

``reverse`` is not carried. v3's only branch on it tests
``assessment_type in ["question-answering", "word-tests"]``, and neither value is in
``AssessmentType`` — so it cannot fire for anything v4 can create. (The comment above it
says "missing words", naming a third type again: a stale comment, not a spec.)
"""

from datetime import datetime
from enum import Enum
from typing import Annotated, Literal, Union

from pydantic import Field, field_validator, model_validator

from api_v4.jobs import JobEnvelope, JobState
from api_v4.schemas.base import V4BaseModel
from schemas.assessment import AssessmentType

#: Generous bound for a verse reference. The longest entry in ``fixtures/vref.txt``
#: is 11 characters (``PSA 119:100``). Bounded rather than free-form because these
#: values land in ``Assessment.kwargs``, which the shared v3 validator caps at 1000
#: characters per string — an over-length value would surface there as an unhandled
#: ``ValueError`` (a 500) instead of a 422 at the edge.
VREF_MAX_LENGTH = 32

#: Bound for the agent's requested response language ("English", "Português do
#: Brasil", an ISO code, ...). Same reasoning as :data:`VREF_MAX_LENGTH`.
RESPONSE_LANGUAGE_MAX_LENGTH = 64


class AssessmentOptionsBase(V4BaseModel):
    """Base for every member of the ``options`` discriminated union.

    Supplies the closed-allowlist config and the :meth:`stored_options` contract.
    Not a union member itself — it declares no ``type`` discriminator.
    """

    model_config = {
        **V4BaseModel.model_config,
        # Closed allowlist. An unknown key, or a key that belongs to a *different*
        # assessment type, is a 422 rather than a silently dropped option — see the
        # module docstring.
        "extra": "forbid",
    }

    def stored_options(self) -> dict:
        """The options this member persists into ``Assessment.kwargs``.

        Returns a fresh dict the caller may mutate. Every member overrides this, so a
        new union member cannot inherit "stores nothing" by accident — it has to say
        what it stores, which is the one place v3's present-or-absent asymmetry is
        easy to get wrong.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must declare what it stores in Assessment.kwargs"
        )


class ReferencedAssessmentOptions(AssessmentOptionsBase):
    """Base for the three types that compare a revision against a reference.

    ``reference_id`` is a required ``int`` here rather than an optional field checked
    at runtime — see the module docstring. The service uses ``isinstance`` against
    this class to decide whether a request names a reference at all, so the
    distinction stays explicit instead of a ``getattr`` default.
    """

    reference_id: int = Field(
        description=(
            "Id of the reference revision this assessment compares against. Required "
            "for this assessment type; the caller must be able to see it."
        ),
    )


class WordAlignmentOptions(ReferencedAssessmentOptions):
    """``word-alignment`` — align the revision's words against the reference's.

    Two runners share this type and are told apart *only* by the stored
    ``use_eflomal`` flag (``assessment_routes/v3/alignment_filters.py``).
    """

    type: Literal["word-alignment"]
    use_eflomal: bool = Field(
        default=True,
        description=(
            "Run the eflomal aligner (the default). Set false to run fastalign "
            "instead. The choice is part of the assessment's identity: an eflomal "
            "and a fastalign run of the same pair are separate assessments."
        ),
    )

    def stored_options(self) -> dict:
        # v3's asymmetry, reproduced exactly: eflomal stores {"use_eflomal": true},
        # fastalign stores no flag at all (assessment_routes.py:505-516). Storing an
        # explicit false instead would still read as fastalign to
        # eflomal_method_clause (which tests containment of {"use_eflomal": true}),
        # but it would NOT match v3's dedup, which compares kwargs for exact
        # equality — so the same request through v3 and v4 would enqueue two runs.
        return {"use_eflomal": True} if self.use_eflomal else {}


class SemanticSimilarityOptions(ReferencedAssessmentOptions):
    """``semantic-similarity`` — sentence-embedding similarity against the reference."""

    type: Literal["semantic-similarity"]
    finetune: bool = Field(
        default=False,
        description=(
            "Fine-tune the similarity model for this pair. Worth setting when the "
            "revision's language is not one the base model covers well."
        ),
    )

    def stored_options(self) -> dict:
        # Present-or-absent, as for use_eflomal: v3 only ever receives this through
        # extra_kwargs, and the client sends it only when it wants it on — so the
        # v3-equivalent of finetune=false is a row with no finetune key. Storing an
        # explicit false would split dedup between v3- and v4-created rows.
        return {"finetune": True} if self.finetune else {}


class AgentCritiqueOptions(ReferencedAssessmentOptions):
    """``agent-critique`` — LLM critique of a verse range against the reference."""

    type: Literal["agent-critique"]
    first_vref: str = Field(
        min_length=1,
        max_length=VREF_MAX_LENGTH,
        description="First verse of the range to critique, e.g. 'GEN 1:1'.",
    )
    last_vref: str | None = Field(
        default=None,
        min_length=1,
        max_length=VREF_MAX_LENGTH,
        description=(
            "Last verse of the range. Omit it to critique through to the end of the "
            "chapter — omitted and 'the whole chapter' are the same request, so this "
            "has no default value."
        ),
    )
    response_language: str | None = Field(
        default=None,
        min_length=1,
        max_length=RESPONSE_LANGUAGE_MAX_LENGTH,
        description="Language the agent should write its critique in.",
    )
    transcribed_audio: bool | None = Field(
        default=None,
        description=(
            "Whether the draft is a transcription of recorded audio (ASR), so the "
            "critique expects surface transcription noise while still flagging "
            "genuine content differences. Omit it to inherit the draft version's own "
            "transcribed_audio setting, which is what makes this tri-state rather "
            "than a plain boolean."
        ),
    )

    @field_validator("first_vref", "last_vref", "response_language")
    @classmethod
    def _reject_blank(cls, value: str | None) -> str | None:
        """Reject a value that is empty or only whitespace.

        ``min_length=1`` alone would let ``"   "`` through, and both spellings are
        load-bearing rather than cosmetic. An empty ``first_vref`` is stored as
        ``{"first_vref": ""}`` but reads as *absent* to the create-time dedup, which
        probes falsy values with ``NOT (kwargs ? 'first_vref')``
        (``assessment_service._completed_duplicate_query``): the stored row has the
        key, the repeat request looks for rows without it, so the two never match and
        an identical resubmit dispatches a second GPU run instead of returning 409.
        A whitespace-only value dedups correctly but is equally meaningless to the
        runner, so both are refused at the edge.

        The value is checked, never stripped: ``Assessment.kwargs`` must stay
        byte-identical to what v3 would have stored for the same request (see the
        module docstring), and silently rewriting a caller's value would break that.
        """
        if value is not None and not value.strip():
            raise ValueError("must not be empty or whitespace-only")
        return value

    def stored_options(self) -> dict:
        """The vref range and response language; ``transcribed_audio`` is *not* here.

        Deliberate split: the effective ``transcribed_audio`` can only be resolved
        against the draft version's own column (#815), which is a database read, so
        :mod:`assessment_routes.v4.assessment_service` overlays that key. Everything
        resolvable from the request alone is resolved here.

        ``last_vref`` and ``response_language`` are omitted when absent rather than
        stored as JSON ``null``: v3 only ever writes keys the caller supplied, and
        the create-time dedup probes them with ``NOT (kwargs ? 'last_vref')`` —
        against which a stored ``null`` counts as *present* and would silently split
        the dedup group.
        """
        stored: dict = {"first_vref": self.first_vref}
        if self.last_vref is not None:
            stored["last_vref"] = self.last_vref
        if self.response_language is not None:
            stored["response_language"] = self.response_language
        return stored


class SentenceLengthOptions(AssessmentOptionsBase):
    """``sentence-length`` — per-verse sentence-length statistics for one revision."""

    type: Literal["sentence-length"]

    def stored_options(self) -> dict:
        return {}


class TextLengthsOptions(AssessmentOptionsBase):
    """``text-lengths`` — per-verse word/character length metrics for one revision.

    Takes no reference even though it powers a *comparison*: the comparison pairs two
    independent single-revision assessments (``results_query_routes.py:1053-1102``).
    """

    type: Literal["text-lengths"]

    def stored_options(self) -> dict:
        return {}


class NgramsOptions(AssessmentOptionsBase):
    """``ngrams`` — n-gram inventory for one revision."""

    type: Literal["ngrams"]

    def stored_options(self) -> dict:
        return {}


class TfidfOptions(AssessmentOptionsBase):
    """``tfidf`` — TF-IDF vectors / neighbours for one revision."""

    type: Literal["tfidf"]

    def stored_options(self) -> dict:
        return {}


#: The ``options`` union, discriminated on ``type``. Pydantic dispatches on the tag
#: rather than trying each member in turn, so a body with an unknown ``type`` reports
#: *that* rather than seven unrelated per-member validation errors. Every value of
#: ``schemas.assessment.AssessmentType`` appears exactly once; a test pins that, so
#: adding an internal type without a v4 options member fails loudly.
AssessmentOptions = Annotated[
    Union[
        WordAlignmentOptions,
        SemanticSimilarityOptions,
        AgentCritiqueOptions,
        SentenceLengthOptions,
        TextLengthsOptions,
        NgramsOptions,
        TfidfOptions,
    ],
    Field(discriminator="type"),
]


class AssessmentCreate(V4BaseModel):
    """Request body for ``POST /v4/assessments`` (issue #826: JSON-only bodies).

    ``force`` is at the top level, not inside ``options``, because it is universal:
    it is a property of *this submission* ("rerun even though a finished one
    exists"), not of the assessment being run, and it is deliberately not stored on
    the row.

    Closed allowlist (``extra="forbid"``) — see the module docstring for why the five
    v3 inputs v4 drops must produce a 422 rather than being silently ignored.
    """

    revision_id: int = Field(
        description=(
            "Id of the revision to assess. The caller must be able to see it; a "
            "revision outside the caller's groups is reported exactly as a "
            "non-existent one."
        ),
    )
    force: bool = Field(
        default=False,
        description=(
            "Rerun even if a finished assessment for the same revision, reference, "
            "type and options already exists. It does not bypass the check for an "
            "assessment of the same shape that is still running."
        ),
    )
    options: AssessmentOptions = Field(
        description=(
            "The assessment type and its type-specific options. `type` selects the "
            "shape; each type documents exactly the options it accepts."
        ),
    )

    model_config = {
        **V4BaseModel.model_config,
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "revision_id": 12,
                "force": False,
                "options": {
                    "type": "word-alignment",
                    "reference_id": 7,
                    "use_eflomal": True,
                },
            }
        },
    }


class AssessmentOut(V4BaseModel):
    """The assessment resource: one row of ``GET /v4/assessments``.

    Built from **named columns** by ``assessment_routes._to_out``, never by splatting
    the ORM row's ``__dict__`` — the same rule :class:`~api_v4.schemas.bible.RevisionOut`
    states, and for the same reason: a closed field set that only *happens* not to leak
    is one config change away from leaking.

    See the module docstring for what this deliberately omits (``status``,
    ``is_training``, ``attempt_count``) and why ``options`` is an open object.
    """

    id: int = Field(
        description=(
            "The assessment's id — an integer, and the same value the job envelope "
            "reports as the string `job_id`."
        ),
    )
    revision_id: int = Field(
        description="Id of the revision that was assessed.",
    )
    reference_id: int | None = Field(
        default=None,
        description=(
            "Id of the reference revision this assessment compared against, or null "
            "for the four types that take no reference."
        ),
    )
    type: AssessmentType = Field(
        description="Which assessment was run.",
    )
    state: JobState = Field(
        description=(
            "The run's current public state; branch on this. This is the public "
            "spelling of the internal status column — v4 translates the vocabulary at "
            "the edge, so the lowercase queued/running/finished/failed values never "
            "appear on the wire."
        ),
    )
    status_detail: str | None = Field(
        default=None,
        description=(
            "Free-text detail the runner last reported — a progress note while "
            "running, or the reason it failed. Prose for a human; do not parse it. On "
            "a FAILED poll the same text is also the job error's `message`."
        ),
    )
    percent_complete: float | None = Field(
        default=None,
        description=(
            "How far along the run is, 0-100, as last reported by the runner. Null "
            "when it has never reported progress. Progress is a field rather than "
            "part of `result`, which carries outcomes only."
        ),
    )
    requested_time: datetime | None = Field(
        default=None,
        description="When the run was submitted.",
    )
    start_time: datetime | None = Field(
        default=None,
        description="When the runner started work, or null if it has not yet.",
    )
    end_time: datetime | None = Field(
        default=None,
        description="When the run reached a terminal state, or null if it has not.",
    )
    owner_id: int | None = Field(
        default=None,
        description=(
            "Id of the user who submitted the run. Null on rows created before the "
            "column existed — which is also why only an admin can delete those."
        ),
    )
    options: dict | None = Field(
        default=None,
        description=(
            "The options this run was created with, as stored: the eflomal/fastalign "
            "choice, a vref range, and so on. Null when the type takes no options. "
            "Deliberately an open object rather than the request body's typed "
            "`options` union — `type` and `reference_id` are top-level fields here, "
            "and a run submitted through v3 may carry keys no v4 assessment type "
            "declares."
        ),
    )
    deleted: bool = Field(
        default=False,
        description=(
            "Whether the assessment has been soft-deleted. Normally false, since "
            "deleted rows are filtered out; they surface for an admin passing "
            "include_deleted, and in an `updated_since` delta window, where a "
            "soft-delete is how a mirror learns to drop the row."
        ),
    )
    # Nullable, and NOT coerced: NULL is meaningful (a legacy row predating the
    # column), and a mirror must be able to tell "never stamped" from a real
    # timestamp — max() skips NULLs, so such a row cannot advance a watermark.
    # Mirrors VersionOut/RevisionOut.updated_at.
    updated_at: datetime | None = Field(
        default=None,
        description=(
            "When the row was last written, and the basis of the `updated_since` "
            "delta feed. Null on legacy rows that predate the column."
        ),
    )


class AssessmentJob(AssessmentOut, JobEnvelope):
    """The poll body for ``GET /v4/assessments/{id}``: the resource *plus* the envelope.

    Adds :class:`~api_v4.jobs.JobEnvelope`'s ``job_id`` / ``result`` / ``error`` to
    :class:`AssessmentOut` (whose ``state`` is the envelope's ``state``), so a single
    poll answers both "what is this job doing" and "what is this assessment". All four
    envelope keys are always present, ``"error": null`` included — so the poll route
    must **not** carry ``response_model_exclude_none=True``.

    Inheriting the envelope rather than restating it is what keeps its invariants
    enforced here: ``error`` is non-null exactly when ``state`` is ``FAILED``, and
    ``result`` is null unless ``state`` is ``SUCCEEDED``. Note that
    :meth:`~api_v4.jobs.JobEnvelope.failed` is *not* usable as a constructor for this
    subclass — it supplies only the envelope's own keys, and the resource fields are
    required. Build the ``error`` with it and pass it in; the response builder does.

    ``result`` is inherited untyped and is null in every state today, which the envelope
    explicitly permits for a ``SUCCEEDED`` job — see the module docstring.
    """


#: Length of a USFM book abbreviation. Every entry in ``fixtures/book_reference.txt`` is
#: exactly three characters (``GEN``, ``1SA``, ``3JN``), which is what makes an exact
#: length the right validation: v3 checks only ``len(book) > 3``, so it accepts ``"G"``
#: and turns it into an empty result set.
BOOK_ABBREVIATION_LENGTH = 3


class ResultAggregate(str, Enum):
    """The level a result set may be rolled up to. v3's ``aggType``, same three values.

    Absent (``None``) means verse level, which is the default and the only level that
    carries ``vrefs``.
    """

    chapter = "chapter"
    book = "book"
    text = "text"


class ResultScope(V4BaseModel):
    """Which slice of a result set to return, and at what level.

    The four parameters are v3's, and so are the invariants — but they hold *by
    construction* here (see the module docstring). Five rules, each of which v3 raises a
    separate ``HTTPException(400)`` for:

    1. ``chapter`` requires ``book``
    2. ``verse`` requires ``chapter`` (and so, transitively, ``book``)
    3. ``aggregate=book`` conflicts with ``chapter``
    4. ``aggregate=chapter`` conflicts with ``verse``
    5. ``aggregate=text`` conflicts with ``book``, ``chapter`` and ``verse``

    Rules 3-5 are all the same statement — a rollup cannot be narrower than the scope it
    rolls up — but they are written out one at a time so the 422's message names the pair
    the caller actually sent.

    A ``book`` that is well-formed but names no book yields an empty page rather than a
    404. It narrows an already-authorized set instead of naming the collection's parent,
    which is the same rule ``GET /v4/assessments``' filters follow.
    """

    book: str | None = Field(
        default=None,
        min_length=BOOK_ABBREVIATION_LENGTH,
        max_length=BOOK_ABBREVIATION_LENGTH,
        description=(
            "Restrict to one book, as its three-letter USFM abbreviation (`MAT`). "
            "Case-insensitive. A well-formed abbreviation that names no book yields an "
            "empty page, not a 404."
        ),
    )
    chapter: int | None = Field(
        default=None,
        ge=1,
        description="Restrict to one chapter. Requires `book`.",
    )
    verse: int | None = Field(
        default=None,
        ge=1,
        description="Restrict to one verse. Requires `book` and `chapter`.",
    )
    aggregate: ResultAggregate | None = Field(
        default=None,
        description=(
            "Roll the scores up to this level instead of returning verses. Omit it for "
            "verse level. An aggregate level cannot be narrower than the scope it "
            "summarizes, so `chapter` excludes `verse`, `book` excludes `chapter`, and "
            "`text` excludes all three."
        ),
    )

    @field_validator("book")
    @classmethod
    def _upper(cls, value: str | None) -> str | None:
        """Normalize the abbreviation to upper case.

        Stored book values come from ``fixtures/vref.txt`` via ``bible_loading``, so they
        are always upper case. Normalizing the *input* once here lets the query compare
        the column directly and use ``idx_assessment_result_main``, where v3's
        ``func.upper(column) == book.upper()`` cannot.
        """
        return value.upper() if value is not None else None

    @model_validator(mode="after")
    def _consistent_scope(self) -> "ResultScope":
        if self.chapter is not None and self.book is None:
            raise ValueError("chapter requires book")
        if self.verse is not None and self.chapter is None:
            raise ValueError("verse requires book and chapter")
        if self.aggregate is ResultAggregate.book and self.chapter is not None:
            raise ValueError("aggregate=book cannot be combined with chapter")
        if self.aggregate is ResultAggregate.chapter and self.verse is not None:
            raise ValueError("aggregate=chapter cannot be combined with verse")
        if self.aggregate is ResultAggregate.text and (
            self.book is not None or self.chapter is not None or self.verse is not None
        ):
            raise ValueError(
                "aggregate=text cannot be combined with book, chapter or verse"
            )
        return self


class AssessmentResultOut(V4BaseModel):
    """One verse-level row of ``GET /v4/assessments/{id}/results``.

    ``vref`` and ``vrefs`` are the reason this read exists in this shape, and they are not
    redundant:

    * **``vref`` is the span's first verse** — ``MAT 9:20`` for a row covering
      ``MAT 9:20-21`` — because that is what is stored and it is the stable key. Labelling
      the row ``MAT 9:20-21`` was considered and rejected on evidence: the existing client
      inner-joins every result set against a canonical ``vref.txt`` fixture, so a row
      whose ``vref`` is not a literal line of that file is dropped with no error, and the
      row would simply vanish from the web app.
    * **``vrefs`` is every verse the row covers**, derived at read time from the
      revision's ``<range>`` markers (:mod:`bible_routes.v4.verse_range_service`). One
      entry in the overwhelming majority of cases.

    What ``vrefs`` is *for*: a result set is not one row per verse, and a missing verse has
    two different causes. It may be covered by the span above it, or it may never have been
    scored at all — verified on revision 24976, which has 1065 merged-text rows and 1064
    result rows, the gap being ``MAT 23:14``, a bracketed textual-variant verse with real
    text that the reference lacks. The union of every row's ``vrefs`` is exactly the
    covered set, so anything outside it is genuinely unassessed. Without the field the two
    cases are indistinguishable, which is why it is not merely a convenience.
    """

    id: int = Field(
        description=(
            "The stored row's id. Stable for a given assessment and verse, but not a "
            "handle: no v4 endpoint addresses a single result row."
        ),
    )
    assessment_id: int = Field(
        description="The assessment these results belong to (echoed from the path).",
    )
    vref: str = Field(
        description=(
            "The verse this row is stored under — the **first** verse of the span when "
            "several were merged. Always a literal canonical vref, so it joins against "
            "`vref.txt` and against the verses read."
        ),
    )
    vrefs: list[str] = Field(
        description=(
            "Every verse this row covers, in canonical order and beginning with `vref`. "
            "A single entry unless the revision merged verses into this one (`<range>`), "
            "in which case the continuations follow. The union of this field across a "
            "whole result set is the assessed set: a verse absent from every `vrefs` was "
            "never scored, rather than being covered by a neighbour."
        ),
    )
    score: float | None = Field(
        default=None,
        description=(
            "The verse's score. Null only if the row was stored without one; what the "
            "number means is the assessment type's business."
        ),
    )
    flag: bool = Field(
        default=False,
        description=(
            "Whether the row was flagged for attention. Coerced from null on legacy "
            "rows written before the column had a default."
        ),
    )
    hide: bool = Field(
        default=False,
        description=(
            "Whether the row is marked as hidden from display. Advisory — the row is "
            "still returned; it is up to the client to honour it."
        ),
    )
    note: str | None = Field(
        default=None,
        description=(
            "Free-text note the runner attached to this verse, or null. New in v4: v3's "
            "grouped projection drops the column, so `/result` always reported it null."
        ),
    )


class AssessmentResultAggregateOut(V4BaseModel):
    """One rolled-up row of ``GET /v4/assessments/{id}/results?aggregate=...``.

    Its own type rather than the verse row with fields nulled — see the module docstring.
    ``vrefs`` is structurally absent: the range merge is verse-level only.

    The rollup rules are v3's, and they are not symmetric:

    * ``score`` is the **mean** of the verses in scope (``avg``).
    * ``flag`` and ``hide`` are **any** (``bool_or``) — one flagged verse flags its whole
      chapter.

    Note this is a different question from what a score does across a *merged verse span*,
    which does not arise at all: no rows exist for merged continuations, so there is
    nothing to combine.
    """

    assessment_id: int = Field(
        description="The assessment these results belong to (echoed from the path).",
    )
    book: str | None = Field(
        default=None,
        description=(
            "The book this row summarizes, or null at `aggregate=text`, which "
            "summarizes everything and so has no location. v3 rendered these as a "
            "`vref` string (`MAT`, `MAT 9`); build that from `book` and `chapter` if you "
            "need it."
        ),
    )
    chapter: int | None = Field(
        default=None,
        description=(
            "The chapter this row summarizes; null at `aggregate=book` and "
            "`aggregate=text`."
        ),
    )
    score: float | None = Field(
        default=None,
        description=(
            "Mean score across the verses in scope. Null only if none of them had a "
            "score."
        ),
    )
    flag: bool = Field(
        default=False,
        description="True if **any** verse in scope was flagged.",
    )
    hide: bool = Field(
        default=False,
        description="True if **any** verse in scope was marked hidden.",
    )


#: The item type of the results page: a verse row, or an aggregated row. Declared as a
#: union rather than one permissive model so ``vrefs`` and ``vref`` cannot appear on an
#: aggregate row and the aggregate shape cannot silently swallow a verse row's fields.
#: Which one a page holds is decided by the request's ``aggregate``, so a client never
#: has to sniff the shape; ``test_assessment_routes_v4`` pins that both directions
#: serialize as their own member.
AssessmentResultRow = Union[AssessmentResultOut, AssessmentResultAggregateOut]


__all__ = [
    "BOOK_ABBREVIATION_LENGTH",
    "RESPONSE_LANGUAGE_MAX_LENGTH",
    "VREF_MAX_LENGTH",
    "AgentCritiqueOptions",
    "AssessmentCreate",
    "AssessmentJob",
    "AssessmentOptions",
    "AssessmentOptionsBase",
    "AssessmentOut",
    "AssessmentResultAggregateOut",
    "AssessmentResultOut",
    "AssessmentResultRow",
    "NgramsOptions",
    "ReferencedAssessmentOptions",
    "ResultAggregate",
    "ResultScope",
    "SemanticSimilarityOptions",
    "SentenceLengthOptions",
    "TextLengthsOptions",
    "TfidfOptions",
    "WordAlignmentOptions",
]
