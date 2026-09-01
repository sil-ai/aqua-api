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

:class:`VerseScope` and :class:`ResultScope` are the request half, and they are where
#486's principle is paid off. v3 guards the same four parameters with
``validate_parameters``, a runtime function that raises five separate
``HTTPException(400)``s and is frozen with the rest of v3. Here the invariants are
``model_validator`` methods, so an inconsistent scope **cannot be constructed**: the
service layer never has to re-check, the rules are unit-testable without an HTTP
client, and the failure is the standard 422 envelope rather than a bespoke 400. The
adapters that turn query parameters into one of these live with the routes, so OpenAPI
still documents flat query parameters rather than one object.

The split is by what a read's row *is*. :class:`VerseScope` holds the three narrowing
parameters and the two rules over them; :class:`ResultScope` adds ``aggregate`` and the
three rules that stop a rollup being narrower than the scope it rolls up. The
word-keyed reads (:class:`AlignmentScoreOut`, :class:`MissingWordOut`) take the base
only — a row there is a word, so there is no per-verse set for a rollup to summarize.

``reverse`` is not carried. v3's only branch on it tests
``assessment_type in ["question-answering", "word-tests"]``, and neither value is in
``AssessmentType`` — so it cannot fire for anything v4 can create. (The comment above it
says "missing words", naming a third type again: a stale comment, not a spec.)


The ngrams read: the one result row that is not a verse
--------------------------------------------------------

``GET /v4/assessments/{id}/ngrams`` (:class:`NgramResultOut`) is the odd member of this
family, and reading it as a sibling of ``/results`` is the mistake to avoid. Its rows are
**n-grams**, each carrying the verses the n-gram occurs in; nothing about it is keyed by
verse. So :class:`ResultScope`, :class:`ResultAggregate` and the ``book`` / ``chapter`` /
``verse`` parameters above are not wired into it — there is no per-verse axis to narrow
and no per-verse set to roll up, and inventing a per-book n-gram filter would be a new
capability rather than a port.

**The verse list is ``occurrences``, not ``vrefs`` — the one deliberate field rename in
the family** (repo owner, 2026-08-28). ``AssessmentResultOut.vrefs`` means the verses a
single merged span covers: a range-merge concept, nearly always one entry, whose job is
joining a score to the text it scored. An n-gram's list is every verse the n-gram was
found in — an occurrence list, potentially hundreds of entries, with no range-merge
meaning. Same name, same parent, sibling endpoints, different meanings, and nothing
errors when a client confuses them; a client that met ``vrefs`` on ``/results`` first
would reasonably assume they agree. The rename costs nothing because there are no v4
clients, and it is a response field name only — the column stays
``ngram_vref_table.vref``.


The similarity read: a ranking, which is not a list
-----------------------------------------------------

``GET /v4/assessments/{id}/similar-verses`` (:class:`SimilarVersesOut`) is the one read
in this family that is not a listing at all, and every convention it breaks follows from
that. It takes a required ``vref``, loads that verse's vector for this assessment, and
ranks every *other* verse in the same assessment against it. The rows are computed
pairings rather than stored rows, so there is no population to count and nothing to page
through: no :class:`~api_v4.pagination.V4Page`, no ``total``, no ``offset``. The envelope
names the query point and the ranking instead.

**The path says what the endpoint does, not how.** ``/similar-verses`` rather than
``/tfidf`` — the operation is "which verses are most like this one", and naming a
resource after the algorithm that computes it describes the implementation. The type gate
keeps it unambiguous (the parent is a ``tfidf`` assessment or the read 404s), and the
same shape would serve any other type that ever offers similarity. A deliberate departure
from guide §15.3's planned ``/tfidf``.

**``reference_id`` is not a parameter.** v3 lets a caller name any revision whose text to
attach, which makes the response depend on a display preference rather than on the
assessment. v4 uses the assessment's own reference where it has one and returns
``reference_text: null`` where it does not. A caller who wants arbitrary verse text
already has the verses read (#892).


The alignment reads: word rows, and the endpoint that stopped existing
-----------------------------------------------------------------------

``GET /v4/assessments/{id}/alignment-scores`` (:class:`AlignmentScoreOut`) and
``GET /v4/assessments/{id}/missing-words`` (:class:`MissingWordOut`) both serve
``word-alignment`` and both read the same table. Their rows are **words** — one source
word aligned to one target word in one verse — which is why neither carries
``aggregate``: a chapter mean over word rows would produce a number that looks like the
one ``/results`` gives for the same assessment and is not it.

**``/alignment-scores`` absorbs v3's ``GET /alignmentmatches`` rather than porting it.**
That endpoint is this read filtered to one ``source`` above one score, so it is a filter
and not a resource. The fold is only lossless because the two verse texts come back on
every row — ``/alignmentmatches`` joins ``verse_text`` twice and dropping them would lose
output. That is deliberately the opposite call from ``/results``, which *dropped* its
text fields: there the parameter that would have filled them was ignored and they were
always null, so nothing was lost.

**``/missing-words`` keeps its own endpoint by the same test**, because it adds fields
derived from *other* assessments — the same test that kept ``/score-comparison`` off
``/results``. :class:`MissingWordTargetOut` is that addition, and it carries the peer's
**assessment** id as well as its revision id, since ``against`` now names assessments
where v3's ``baseline_ids`` named revisions.

**``missing-words`` is not an assessment type** — no enum value, no table, no runner.
Both reads take a ``word-alignment`` assessment id. The client's ``missing-words`` is a
UI category, and "pass the id of the missing-words assessment" is the natural and wrong
guess, so both the endpoint and :class:`MissingWordOut` say so.

:class:`AlignmentScoreType` chooses *which stored rows* to read, not how to read them;
the two tables hold different things, and there is no fallback between them.


The text-lengths read: the same two shapes, over a table that stores only ``vref``
----------------------------------------------------------------------------------

``GET /v4/assessments/{id}/text-lengths`` (:class:`TextLengthsOut`,
:class:`TextLengthsAggregateOut`, :data:`TextLengthsRow`) is deliberately the closest
thing in the family to ``/results``: the same verse-level/aggregate split, the same
``vref``/``vrefs`` labelling, the same :class:`ResultScope`, and a union for the same
reason. What differs is entirely below the wire — ``text_lengths_table`` carries no
``book``/``chapter``/``verse`` columns, so canonical order and the span-map lookup both
come from a join through the reference tables rather than from stored values. None of
that is visible in these schemas, which is the point.

**The rollup's z-scores are means of per-verse z-scores, and both fields say so.** That is
the one place a reader can be silently wrong about what a number means: ``word_lengths_z``
on a chapter row averages the chapter's verses' z-scores; it is not the chapter's own
z-score against a distribution of chapters, and no such distribution exists anywhere in
this system. v3 returns the same number and documents nothing, so a client that has been
reading it as the second thing has been over-reading it. Both are defensible statistics;
only one is what the field name suggests, so the description carries the correction rather
than a code comment.

``include_text`` is not ported, and it is dead twice over: ``GET /text_lengths_result``
does not declare the parameter, so FastAPI discards it, and v3's ``TextLengthsResult`` has
no text field it could have filled anyway. ``aqua-django-app`` sends it regardless. Dead
v3 parameters do not come into v4 — not carried, not deprecated, not "for parity".


The score-comparison read: the same two shapes, plus the one envelope that grew a field
---------------------------------------------------------------------------------------

``GET /v4/assessments/{id}/score-comparison`` (:class:`ScoreComparisonOut`,
:class:`ScoreComparisonAggregateOut`, :data:`ScoreComparisonRow`) is ``/results``' verse
row and aggregate row with four fields added — ``mean_score``, ``stdev_score``,
``z_score`` and ``baseline_count`` — and three dropped: ``flag``, ``hide`` and ``note``,
all properties of the subject's stored row alone, which ``/results`` already serves.

Two things a reader should take from these models rather than discover:

**What a union of ``vrefs`` tells you is narrower here.** The field itself means the
same on all six row models that carry one — :class:`AssessmentResultOut`,
:class:`TextLengthsOut`, :class:`AlignmentScoreOut`, :class:`MissingWordOut`,
:class:`ScoreComparisonOut` and the verses read's ``VerseOut``: the verses this row covers,
read off its revision's span map. What differs is what the *union* across a result set
means. On ``/results`` and ``/text-lengths``, where a row is a verse and every scored verse
has one, that union is the *assessed* (or measured) population. Here it is not: a verse can
be scored on both sides and still uncompared, because the two revisions merge it
differently. The name and type are identical, so nothing errors when the two readings are
confused — which is why the difference is stated on the field as well as here.

**``z_score`` null at one baseline is arithmetic, not a gap.** ``stdev_score`` is the
sample standard deviation, undefined at n = 1, so a single peer yields no z-score. v3
reaches the same answer and documents none of it, which leaves a client to discover that
naming one baseline produces a column of nulls.

:class:`ScoreComparisonPage` is the only model here that is not a plain body or row: it
subclasses :class:`~api_v4.pagination.V4Page` to add ``against_assessment_ids``, because
the path names the subject and something has to name the peers. The subclass rather than a
standalone model, and the shared envelope left untouched, are argued on the class itself.
"""

from datetime import datetime
from enum import Enum
from typing import Annotated, Literal, Union

from pydantic import Field, field_validator, model_validator

from api_v4.jobs import JobEnvelope, JobState
from api_v4.pagination import V4Page
from api_v4.schemas.base import V4BaseModel

# A book abbreviation being three characters is a fact about the *Bible* domain, not
# about assessments, so it is defined once in ``api_v4.schemas.bible`` and imported
# here rather than the other way round — the verses read (#892) scopes by book too.
# It stays in this module's ``__all__``, so existing importers are unaffected.
from api_v4.schemas.bible import BOOK_ABBREVIATION_LENGTH
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


class ResultAggregate(str, Enum):
    """The level a result set may be rolled up to. v3's ``aggType``, same three values.

    Absent (``None``) means verse level, which is the default and the only level that
    carries ``vrefs``.
    """

    chapter = "chapter"
    book = "book"
    text = "text"


class VerseScope(V4BaseModel):
    """Which verses of a verse-keyed result set to return.

    The three parameters are v3's, and so are the invariants — but they hold *by
    construction* here (see the module docstring). Two rules, each of which v3 raises a
    separate ``HTTPException(400)`` for:

    1. ``chapter`` requires ``book``
    2. ``verse`` requires ``chapter`` (and so, transitively, ``book``)

    A ``book`` that is well-formed but names no book yields an empty page rather than a
    404. It narrows an already-authorized set instead of naming the collection's parent,
    which is the same rule ``GET /v4/assessments``' filters follow.

    Split out of :class:`ResultScope` rather than duplicated because four reads on this
    parent take these three parameters and only ``/results`` and ``/text-lengths`` also
    take ``aggregate``. One implementation means the narrowing rules cannot drift between
    reads, and a reader comparing two endpoints' 422s finds one rule rather than two
    copies of it.
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
    def _consistent_scope(self) -> "VerseScope":
        if self.chapter is not None and self.book is None:
            raise ValueError("chapter requires book")
        if self.verse is not None and self.chapter is None:
            raise ValueError("verse requires book and chapter")
        return self


class ResultScope(VerseScope):
    """:class:`VerseScope` plus the rollup level, for the two reads that aggregate.

    Adds three rules to the base's two, each of which v3 also raises a separate
    ``HTTPException(400)`` for:

    3. ``aggregate=book`` conflicts with ``chapter``
    4. ``aggregate=chapter`` conflicts with ``verse``
    5. ``aggregate=text`` conflicts with ``book``, ``chapter`` and ``verse``

    All three are the same statement — a rollup cannot be narrower than the scope it
    rolls up — but they are written out one at a time so the 422's message names the pair
    the caller actually sent.

    The two validators both run: they have different names, so the subclass's is an
    addition rather than an override, and a request violating a base rule and an
    aggregate rule at once is rejected by the base rule first.
    """

    aggregate: ResultAggregate | None = Field(
        default=None,
        description=(
            "Roll the scores up to this level instead of returning verses. Omit it for "
            "verse level. An aggregate level cannot be narrower than the scope it "
            "summarizes, so `chapter` excludes `verse`, `book` excludes `chapter`, and "
            "`text` excludes all three."
        ),
    )

    @model_validator(mode="after")
    def _aggregate_is_not_narrower(self) -> "ResultScope":
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


class NgramResultOut(V4BaseModel):
    """One row of ``GET /v4/assessments/{id}/ngrams``.

    **A row here is an n-gram, not a verse**, and that is the one thing to internalise
    before reading the rest. Every other typed result sub-resource on this parent is keyed
    by verse; this one is keyed by the n-gram, and each row carries the list of verses the
    n-gram was found in. That is why ``book`` / ``chapter`` / ``verse`` and ``aggregate``
    are not accepted by the endpoint — there is nothing per-verse to scope or roll up.

    ``occurrences`` is deliberately **not** called ``vrefs``, and the rename is the reason
    this docstring exists. ``AssessmentResultOut.vrefs`` on the sibling ``/results`` read
    means *the verses one merged span covers* — a range-merge concept, almost always a
    single entry, whose purpose is joining a score to the text that was scored. This field
    means *every verse in which this n-gram occurs*: an occurrence list, potentially
    hundreds of entries, with no range-merge meaning at all. Two fields with the same name
    and different meanings on sibling endpoints of the same parent resource is a collision
    a client discovers only by getting wrong answers, since nothing errors when they are
    confused. v3 called this field ``vrefs``; v4 renames it because there are no clients
    to inconvenience and the collision is silent.

    ``ngram`` and ``ngram_size`` are required here even though both columns are nullable,
    which is a deliberate match to v3's own ``NgramResult`` rather than an oversight. The
    table's only writer is the runner-facing ``push_ngrams``, whose ``NgramItem`` requires
    both, so a null can only come from a direct database write — and there is no honest
    default to coerce a missing n-gram to, the way ``flag`` and ``hide`` coerce to false
    on ``/results``. Such a row is corrupt data and surfaces as a 500, on both surfaces.
    """

    id: int = Field(
        description=(
            "The stored row's id. Row order is by this column, but it is not a handle: "
            "no v4 endpoint addresses a single n-gram."
        ),
    )
    assessment_id: int = Field(
        description="The assessment this n-gram belongs to (echoed from the path).",
    )
    ngram: str = Field(
        description="The n-gram itself, exactly as the runner stored it.",
    )
    ngram_size: int = Field(
        description="How many tokens the n-gram has — the *n*.",
    )
    occurrences: list[str] = Field(
        description=(
            "Every verse in which this n-gram occurs, as canonical vrefs. **Not** the "
            "span coverage `/results` calls `vrefs`: these are occurrences, so the list "
            "may hold hundreds of entries and carries no range-merge meaning. Empty for "
            "an n-gram stored with no verse references, which is returned rather than "
            "omitted — see the endpoint description."
        ),
    )


class SimilarVerseOut(V4BaseModel):
    """One neighbour in ``GET /v4/assessments/{id}/similar-verses``.

    Not a stored resource. A row here is a *pairing* — this verse, ranked against the
    verse the caller asked about — so it has no id and nothing addresses it. That is why
    it carries neither the ``tfidf_pca_vector`` row's ``id`` nor ``assessment_id``, both
    of which v3 returns: the id identifies a 300-dimensional vector that is not in the
    response, and the assessment is already named by the path and echoed by the envelope.
    ``similarity`` is a property of the pair rather than of the row, so the row is not a
    stable entity an id could name — ask about a different ``vref`` and every number
    changes.
    """

    vref: str = Field(
        description=(
            "The neighbouring verse, as a canonical vref. Never the queried verse "
            "itself, which is excluded from its own ranking."
        ),
    )
    similarity: float = Field(
        description=(
            "How close this verse is to the queried one — the inner product of their "
            "300-dimensional PCA-reduced TF-IDF vectors, higher being more similar. "
            "**A ranking score, not a calibrated one**: it has no fixed range, and "
            "values are comparable within one response but not across assessments, "
            "which are vectorized independently. Do not threshold on it."
        ),
    )
    text: str | None = Field(
        default=None,
        description=(
            "The assessed revision's text for this verse, so a ranked list can be "
            "rendered without a request per hit. Null only if the revision has no row "
            "for the verse."
        ),
    )
    reference_text: str | None = Field(
        default=None,
        description=(
            "The same verse in the assessment's own reference revision, for "
            "side-by-side display. Null for every hit when the assessment has no "
            "reference, which is the normal case for this type — not an error, and not "
            "something a caller can override: v3's `reference_id` parameter is gone."
        ),
    )


class SimilarVersesOut(V4BaseModel):
    """The body of ``GET /v4/assessments/{id}/similar-verses``: a ranking, not a page.

    Deliberately **not** :class:`~api_v4.pagination.V4Page`, and the difference is the
    point. Every other read in this family lists rows that exist in a table, so ``total``
    answers "how many are there" and ``offset`` walks them. This one computes a ranking
    against a query point: the rows do not pre-exist, there is no population to count,
    and a ``total`` equal to ``limit`` would be a number that is present, technically
    defensible and misleading. There is **no ``offset``** either — v3 has never had one
    (its client sends a ``page`` parameter that ``/tfidf_result`` does not declare, so
    FastAPI has always discarded it), and paging a similarity ranking is not something
    anyone has asked for.

    The envelope names the query point instead, because a response that omitted it would
    be uninterpretable on its own: ``similarity`` means nothing without knowing what it is
    similar *to*, and a client holding several of these needs to tell them apart.
    """

    query_vref: str = Field(
        description=(
            "The verse the ranking was computed against, echoed from the request. Every "
            "`similarity` in `items` is relative to this verse."
        ),
    )
    limit: int = Field(
        ge=1,
        description=(
            "The maximum number of neighbours requested, echoed from the request. "
            "`items` may be shorter — an assessment with fewer vectors than this has "
            "fewer neighbours to offer."
        ),
    )
    items: list[SimilarVerseOut] = Field(
        description=(
            "The neighbours, most similar first. Ties break on `vref`, so the same "
            "request twice returns the same ordering."
        ),
    )


class AlignmentScoreType(str, Enum):
    """Which of the two word-alignment score tables ``/alignment-scores`` reads.

    Not a display preference and not interchangeable: the runner writes both, and they
    hold *different rows*. ``top`` is ``alignment_top_source_scores`` — the single
    best-scoring target for each source word in each verse, so ``(vref, source)`` is a
    natural key. ``threshold`` is ``alignment_threshold_scores`` — every target that
    scored above the runner's cutoff, so one source word in one verse can appear several
    times with different targets. Confirmed against production: 31,038 sampled
    ``top`` rows have no duplicate ``(vref, source)`` at all, while the same assessments'
    ``threshold`` rows have thousands.

    v3's enum of the same name (``results_query_routes.py``) has the same two values.
    Redeclared here rather than imported because that module is frozen v3 code and a v4
    wire contract should not be able to change underneath by an edit there.
    """

    top = "top"
    threshold = "threshold"


class AlignmentScoreOut(V4BaseModel):
    """One row of ``GET /v4/assessments/{id}/alignment-scores``.

    **A row is a word pairing, not a verse.** ``/results`` gives one row per verse for the
    same assessment; this gives one row per aligned *word*, so a single verse contributes
    as many rows as it has source words. That is why there is no ``aggregate`` here — see
    the endpoint description.

    This row shape is also what lets v3's ``GET /alignmentmatches`` disappear rather than
    be ported: that endpoint is this read filtered to one ``source`` above one score, and
    the only fields it returned that a bare score row lacks are the two verse texts, which
    are always populated here.
    """

    id: int = Field(
        description=(
            "The stored row's id. Part of the ordering tiebreak, but not a handle: no v4 "
            "endpoint addresses a single alignment row."
        ),
    )
    assessment_id: int = Field(
        description="The assessment this alignment belongs to (echoed from the path).",
    )
    vref: str = Field(
        description=(
            "The verse this alignment was found in — the **first** verse of the span "
            "when the revision merged several. Always a literal canonical vref."
        ),
    )
    vrefs: list[str] = Field(
        description=(
            "Every verse this row covers, in canonical order and beginning with `vref`. "
            "A single entry unless the revision merged verses into this one (`<range>`), "
            "in which case the continuations follow — the same field, derived the same "
            "way, as on `/results`."
        ),
    )
    source: str = Field(
        description=(
            "The source-side word, as the runner stored it — lower-cased. Match it with "
            "the `source` query parameter, which is case-insensitive."
        ),
    )
    target: str | None = Field(
        default=None,
        description="The target-side word this source word aligned to.",
    )
    score: float | None = Field(
        default=None,
        description=(
            "The alignment score for this word pair, higher being a stronger alignment. "
            "Filter on it with `min_score`."
        ),
    )
    flag: bool = Field(
        default=False,
        description=(
            "Whether the row was flagged for attention. Coerced from null on legacy rows "
            "written before the column had a default — the shape that once 500'd v3's "
            "`/alignmentscores`."
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
        description="Free-text note the runner attached to this row, or null.",
    )
    text: str | None = Field(
        default=None,
        description=(
            "The assessed revision's stored text for `vref`, so a word list is "
            "renderable without a request per row. Where `vrefs` lists several verses "
            "this is the **whole merged span's** text, which is how a vref-aligned "
            "upload stores it — the continuations hold the `<range>` marker and carry no "
            "text of their own. So this is exactly the text the alignment ran over. Null "
            "if the revision has no row for the verse."
        ),
    )
    reference_text: str | None = Field(
        default=None,
        description=(
            "The same verse in the assessment's reference revision, for side-by-side "
            "display. Word alignment always has a reference, so this is null only where "
            "the reference lacks the verse."
        ),
    )


class MissingWordTargetOut(V4BaseModel):
    """What one peer assessment had for a word ``GET …/missing-words`` reports as missing.

    Present for **every** peer named in ``against``, whether or not it had anything to
    say — a peer that produced no translation for the word is reported with
    ``target: null`` rather than omitted, because its silence is part of the evidence.
    v3 pads the list the same way, keyed by revision id alone.
    """

    assessment_id: int = Field(
        description=(
            "The peer assessment, as named in `against`. New in v4: v3 identified a peer "
            "only by its revision, because `against` did not exist and peers were "
            "resolved from revision ids."
        ),
    )
    revision_id: int = Field(
        description=(
            "The revision that peer assessed — v3's only peer identifier, kept because "
            "it is what a client joins against to name the translation."
        ),
    )
    target: str | None = Field(
        default=None,
        description=(
            "The word this peer aligned the source word to, or null. **Null has two "
            "causes and they are not distinguished**, exactly as in v3: the peer had no "
            "alignment for this word at this verse, or it had one scoring below the "
            "match threshold and so too weak to count as a translation."
        ),
    )


class MissingWordOut(V4BaseModel):
    """One row of ``GET /v4/assessments/{id}/missing-words``.

    A source word that this assessment aligned *poorly* (below ``max_score``), together
    with what each peer assessment made of the same word. The row is a word, as on
    ``/alignment-scores``; what it adds is the ``targets`` column derived from other
    assessments, which is why it is its own sub-resource rather than a filter.

    ``target`` is deliberately **not** a field here, even though the underlying row has
    one: a word this assessment scored below the threshold has no target worth reporting,
    and the interesting targets are the peers'. v3 reuses its generic ``Result`` schema
    and so returns the peer list under the name ``target``; ``targets`` says what it is.
    """

    id: int = Field(
        description=(
            "The stored ``alignment_top_source_scores`` row's id. Not a handle: no v4 "
            "endpoint addresses a single row."
        ),
    )
    assessment_id: int = Field(
        description=(
            "The **subject** word-alignment assessment (echoed from the path), not a "
            "peer. Peers are identified inside `targets`."
        ),
    )
    vref: str = Field(
        description=(
            "The verse this word was found in — the first verse of the span when the "
            "revision merged several. Always a literal canonical vref."
        ),
    )
    vrefs: list[str] = Field(
        description=(
            "Every verse this row covers, beginning with `vref`. Derived from the "
            "revision's `<range>` markers, exactly as on `/results`."
        ),
    )
    source: str = Field(
        description="The source-side word that appears to be missing, lower-cased.",
    )
    score: float | None = Field(
        default=None,
        description=(
            "How well this assessment aligned the word — below `max_score` by "
            "construction, which is what put the row in this list."
        ),
    )
    flag: bool = Field(
        default=False,
        description=(
            "Whether the peers make this look like a genuine omission rather than a "
            "scoring artefact. True when the mean peer score is **above 0.35** *and* "
            "**more than five times** this assessment's score: the word is well aligned "
            "in the peers and much better aligned there than here. False whenever no "
            "peer had a row for the word, so an unflagged row with an empty or all-null "
            "`targets` means *no evidence*, not *evidence of nothing*. v3's rule, "
            "unchanged; this is not the stored `flag` column, which this read ignores."
        ),
    )
    targets: list[MissingWordTargetOut] = Field(
        description=(
            "One entry per assessment named in `against`, in the order given, including "
            "peers that had nothing for this word. Empty when `against` was not passed — "
            "in which case `flag` is always false and this read is just "
            "`/alignment-scores` filtered to low scores."
        ),
    )


class TextLengthsOut(V4BaseModel):
    """One verse-level row of ``GET /v4/assessments/{id}/text-lengths``.

    The same ``vref`` / ``vrefs`` pair as :class:`AssessmentResultOut`, for the same
    reason and with the same meaning — see that class for the argument. It applies here
    unchanged: ``text_lengths_table`` holds **no** row for a ``<range>`` continuation, so a
    verse missing from a page is either covered by the row above it or was never measured,
    and ``vrefs`` is what tells those apart.

    That was confirmed twice over, at the data and at the source. Assessment 31038's
    production check found no range-vref rows; and the runner cannot write one, because it
    loads text from ``GET /v3/text``, whose default ``include_verses=union`` merges the
    span *before* the runner sees it and reports the anchor as
    ``first_verse_reference``. So the anchor row's measurements are the whole span's text,
    which is what makes ``vrefs`` an honest claim about coverage rather than a label over a
    number that excludes some of it.

    What is different is where the label comes from. ``assessment_result`` stores
    ``book``/``chapter``/``verse`` alongside its ``vref``; this table stores **only**
    ``vref``, so the read reaches ``verse_reference`` → ``chapter_reference`` →
    ``book_reference`` to place a row in canonical Bible order and to key into the span
    map. ``vref`` itself is served from the stored column rather than rebuilt, because
    here it *is* the natural key: it is the foreign key the join follows, so it cannot be
    null on a returned row and cannot disagree with the triple the row was ordered on.

    All four measures are the revision's own — there is no reference side to this
    assessment type, and ``TextLengthsOptions`` has no ``reference_id`` to give it one.

    **All four are nullable, and that is a fix rather than a looser contract.** The columns
    are nullable and the runner-facing push requires all four, so a null can only come from
    a direct database write — but v3 handles that case *incorrectly* rather than not at all:
    its ``TextLengthsResult`` declares them as required ``float`` while
    ``get_text_lengths`` passes ``None`` for a null column, so such a row is a
    ``ValidationError`` and a 500 on v3. Here the verse is still identified and the row still
    reports which verses it covers, so serving the measures as null keeps the coverage
    information a refusal would throw away. It also has to match
    :class:`TextLengthsAggregateOut`, where ``avg`` over an all-null group is null whatever
    this model says; a required field at one level and a nullable one at the other would be
    the worse asymmetry. Contrast :class:`NgramResultOut`, which *does* require its columns:
    there the identifying value itself is the missing one.
    """

    id: int = Field(
        description=(
            "The stored ``text_lengths_table`` row's id. Not a handle: no v4 endpoint "
            "addresses a single row."
        ),
    )
    assessment_id: int = Field(
        description="The assessment these measurements belong to (echoed from the path).",
    )
    vref: str = Field(
        description=(
            "The verse this row measures — the **first** verse of the span when the "
            "revision merged several, in which case the measurements are the whole "
            "span's. Always a literal canonical vref, so it joins against `vref.txt`."
        ),
    )
    vrefs: list[str] = Field(
        description=(
            "Every verse this row covers, in canonical order and beginning with `vref`. "
            "A single entry unless the revision merged verses into this one (`<range>`). "
            "The union of this field across a whole page set is the measured set: a "
            "verse absent from every `vrefs` was never measured, rather than being "
            "covered by a neighbour."
        ),
    )
    word_lengths: float | None = Field(
        default=None,
        description=(
            "How many words the verse holds. **Always a whole number**: the runner counts "
            "with a plain whitespace split, with no preprocessing, so standalone "
            "punctuation counts as a word. Typed as a number rather than an integer "
            "because the column is `NUMERIC` and the aggregated form of this field is a "
            "mean. Null only if the row was stored without the measure."
        ),
    )
    char_lengths: float | None = Field(
        default=None,
        description=(
            "How many characters the verse holds, counted as-is. **Always a whole "
            "number**, and typed as a number for the same reason as `word_lengths`. Null "
            "only if the row was stored without the measure."
        ),
    )
    word_lengths_z: float | None = Field(
        default=None,
        description=(
            "The verse's word count as a z-score — how unusually long or short the verse "
            "is for this translation. **Computed by the runner and stored as-is**; this "
            "read never recomputes it, and the population it was standardized over is the "
            "runner's choice rather than something this API defines."
        ),
    )
    char_lengths_z: float | None = Field(
        default=None,
        description=(
            "The verse's character count as a z-score, with the same caveat as "
            "`word_lengths_z`: stored, not recomputed."
        ),
    )


class TextLengthsAggregateOut(V4BaseModel):
    """One rolled-up row of ``GET /v4/assessments/{id}/text-lengths?aggregate=...``.

    Its own type rather than the verse row with fields nulled, for the reason the module
    docstring gives for :class:`AssessmentResultAggregateOut`: ``vrefs`` is structurally
    absent because the range merge is verse-level only, and so are ``vref`` and ``id`` —
    an aggregate row is not a stored row, so the lowest id among the verses it summarizes
    identifies nothing it represents. (v3 projects ``min(id)`` here and calls it ``id``.)

    **Every measure rolls up as a plain mean, the z-scores included, and that last part
    is the thing to read before using them.** ``word_lengths_z`` on a chapter row is *the
    mean of that chapter's verses' z-scores* — not the chapter's own z-score against a
    distribution of chapters. The two are different numbers and only the second is what
    "the chapter's z-score" usually means. v3 computes the first silently; v4 computes the
    same number and says which one it is, here and in the field descriptions, because a
    reader who assumes the second will read a near-zero mean as "this chapter is typical"
    when it in fact says "this chapter's verses are individually typical for the
    revision", which is a weaker claim.

    ``chapter`` is an integer here, where v3's text-lengths rollup returns it as a string
    (it comes out of ``split_part`` on the vref). Normalized to match ``/results`` and the
    ``chapter`` query parameter, so a client compares what it sent.
    """

    assessment_id: int = Field(
        description="The assessment these measurements belong to (echoed from the path).",
    )
    book: str | None = Field(
        default=None,
        description=(
            "The book this row summarizes, or null at `aggregate=text`, which summarizes "
            "everything and so has no location. v3 rendered these as a single `vref` "
            "string (`MAT`, `MAT 9`) with no `book` or `chapter` field at all; build that "
            "string from `book` and `chapter` if you need it. Worth checking against your "
            "own code rather than skimming: a v3 client that recovers the book and chapter "
            "by splitting that string apart can drop the split entirely, because `chapter` "
            "arrives as the same integer it sent in the request."
        ),
    )
    chapter: int | None = Field(
        default=None,
        description=(
            "The chapter this row summarizes; null at `aggregate=book` and "
            "`aggregate=text`. An integer, where v3's `vref` string carried it as text."
        ),
    )
    word_lengths: float | None = Field(
        default=None,
        description=(
            "Mean word count across the verses in scope. Null only if none of them had "
            "the measure."
        ),
    )
    char_lengths: float | None = Field(
        default=None,
        description=(
            "Mean character count across the verses in scope. Null only if none of them "
            "had the measure."
        ),
    )
    word_lengths_z: float | None = Field(
        default=None,
        description=(
            "**The mean of the verses' own z-scores**, not this scope's z-score against "
            "a distribution of scopes. A value near zero says these verses are each "
            "typical for the revision; it does not say the chapter or book is typical "
            "compared with other chapters or books — no such distribution is computed "
            "anywhere. v3 returns this same number without saying so."
        ),
    )
    char_lengths_z: float | None = Field(
        default=None,
        description=(
            "**The mean of the verses' own character-length z-scores**, with the same "
            "caveat as `word_lengths_z`: it averages per-verse z-scores rather than "
            "scoring this scope against its peers."
        ),
    )


#: The item type of the text-lengths page: a verse row, or an aggregated row. A union for
#: exactly the reason :data:`AssessmentResultRow` is one — ``vref``/``vrefs``/``id`` must
#: be *absent* under aggregation rather than conventionally null, and the aggregate shape
#: must not be able to swallow a verse row. Which one a page holds is decided by the
#: request's ``aggregate``, so a client never has to sniff the shape.
TextLengthsRow = Union[TextLengthsOut, TextLengthsAggregateOut]


class ScoreComparisonOut(V4BaseModel):
    """One verse-level row of ``GET /v4/assessments/{id}/score-comparison``.

    An :class:`AssessmentResultOut` row with the peer distribution attached, and
    deliberately so: ``id``, ``vref``, ``vrefs`` and ``score`` are the *same values* that
    read returns for the same verse, produced by the same query. What is added is the
    four fields that say how this score sits among the peers named by ``against``.

    ``flag``, ``hide`` and ``note`` are **not** carried across, and their absence is a
    decision rather than an oversight. All three are properties of the subject's stored
    row alone, so a caller who wants them is asking a single-assessment question and
    ``/results`` already answers it; repeating them here would make this read look like a
    superset of that one, which it is not — it is that read plus a comparison, over the
    rows the comparison can place. v3 does not return them either.

    **``vrefs`` means something narrower here than on ``/results``, and that is the one
    thing to read before using it for coverage.** There it is the *assessed* population:
    a verse in no row's ``vrefs`` was never scored. Here it is the *comparable*
    population, which is smaller. A verse can be perfectly well assessed on both sides and
    still be absent, or present with ``baseline_count`` 0, because the two revisions merge
    it differently — see the endpoint description for the span rule that decides this.
    """

    id: int = Field(
        description=(
            "The subject's stored ``assessment_result`` row id — the same id "
            "`/results` reports for this verse. Not a handle: no v4 endpoint addresses "
            "a single result row."
        ),
    )
    assessment_id: int = Field(
        description=(
            "The subject assessment (echoed from the path). The peers are named once "
            "for the whole page, in the envelope's `against_assessment_ids`, rather "
            "than repeated on every row."
        ),
    )
    vref: str = Field(
        description=(
            "The verse this row is stored under — the **first** verse of the span when "
            "the revision merged several. Always a literal canonical vref."
        ),
    )
    vrefs: list[str] = Field(
        description=(
            "Every verse this row covers, in canonical order and beginning with `vref`. "
            "A single entry unless the subject's revision merged verses into this one "
            "(`<range>`). **The union across a page set is the *comparable* population, "
            "not the assessed one** — narrower than the same field on `/results`. A "
            "verse missing here may be scored on both sides and simply not comparable."
        ),
    )
    score: float | None = Field(
        default=None,
        description=(
            "The subject's score for this verse — the same number `/results` returns. "
            "Null only if the row was stored without one; what it means is the "
            "assessment type's business."
        ),
    )
    mean_score: float | None = Field(
        default=None,
        description=(
            "Mean of the contributing peers' scores at this verse. Null when no peer "
            "contributed. Each peer counts once however many rows it has here, so a "
            "peer is one observation rather than one per verse."
        ),
    )
    stdev_score: float | None = Field(
        default=None,
        description=(
            "**Sample** standard deviation of those peer scores — Postgres' `stddev`, "
            "which is `stddev_samp`. Null at fewer than two contributing peers, because "
            "a single observation says nothing about spread. That is a definition, not "
            "a missing value to substitute zero for."
        ),
    )
    z_score: float | None = Field(
        default=None,
        description=(
            "How many standard deviations the subject's `score` sits from `mean_score`. "
            "**Null whenever it cannot be computed**, and there are four such cases: no "
            "subject score, no peer contributed, exactly one peer contributed (so "
            "`stdev_score` is null), or every peer scored the verse identically (so it "
            "is zero). One baseline therefore never yields a z-score — expected, not a "
            "bug, and v3 answers the same way without saying so."
        ),
    )
    baseline_count: int = Field(
        description=(
            "How many peers actually contributed to the three fields above. **New in "
            "v4, and worth reading on every row**: v3 reports a mean with no way to "
            "tell five peers from one. A peer is absent from the count when it has no "
            "row at this verse, when its rows here carry no score, or when its revision "
            "merges this verse differently from the subject's — see the endpoint on the "
            "last of those. `0` means the row is uncompared, not that the peers agreed."
        ),
    )


class ScoreComparisonAggregateOut(V4BaseModel):
    """One rolled-up row of ``GET …/score-comparison?aggregate=...``.

    Its own type rather than the verse row with fields nulled, for the reason the module
    docstring gives for :class:`AssessmentResultAggregateOut`: ``id``, ``vref`` and
    ``vrefs`` are *absent* here, not null — an aggregate row is not a stored row, and the
    range merge is verse-level only.

    ``score`` is the same mean ``/results`` reports for this scope, from the same query.
    The three baseline fields are then computed **across peers over that scope**, each
    peer first rolled up to one value the same way the subject was — so a peer is one
    observation whether it contributed a verse or a book.

    **No span test applies under a rollup**, which is the one place this shape is weaker
    than the verse-level one. Where two revisions merge verses differently, the subject
    averages one row over a span while the peer averages two, and nothing here can tell.
    The verse level refuses that comparison; a rollup cannot, because it has no per-verse
    row to refuse. v3 behaves this way at every level.
    """

    assessment_id: int = Field(
        description="The subject assessment (echoed from the path).",
    )
    book: str | None = Field(
        default=None,
        description=(
            "The book this row summarizes, or null at `aggregate=text`, which "
            "summarizes everything and so has no location. v3 rendered these as a "
            "`vref` string (`MAT`, `MAT 9`); build that from `book` and `chapter` if "
            "you need it."
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
            "Mean of the subject's scores across the verses in scope — the same number "
            "`/results` returns for this scope. Null only if none of them had a score."
        ),
    )
    mean_score: float | None = Field(
        default=None,
        description=(
            "Mean across the contributing peers of each peer's own mean over this "
            "scope. Null when no peer contributed."
        ),
    )
    stdev_score: float | None = Field(
        default=None,
        description=(
            "Sample standard deviation of those per-peer means, with the same "
            "null-at-one-peer definition as on a verse row."
        ),
    )
    z_score: float | None = Field(
        default=None,
        description=(
            "How many standard deviations this scope's `score` sits from `mean_score`, "
            "or null in the four cases the verse row lists. Note this is a z-score of "
            "the scope against *its peers' same scope* — a real distribution over "
            "assessments, unlike `/text-lengths`' rolled-up z-scores, which are means "
            "of per-verse z-scores."
        ),
    )
    baseline_count: int = Field(
        description=(
            "How many peers contributed a value for this scope. A peer with no "
            "placeable scored row in it is absent from all three fields above."
        ),
    )


#: The item type of the score-comparison page: a verse row, or an aggregated row. A union
#: for exactly the reason :data:`AssessmentResultRow` is one — ``id``/``vref``/``vrefs``
#: must be *absent* under aggregation rather than conventionally null. Which one a page
#: holds is decided by the request's ``aggregate``, so a client never has to sniff it.
ScoreComparisonRow = Union[ScoreComparisonOut, ScoreComparisonAggregateOut]


class ScoreComparisonPage(V4Page[ScoreComparisonRow]):
    """``V4Page`` plus the one thing a comparison response cannot leave out: the peers.

    Q2 §4 requires both sides of the comparison to be named in the response, and the path
    only names one. The example it gives was written for a read that was later withdrawn,
    so the shape is decided here.

    **A subclass rather than a purpose-built model**, and the deciding fact is that this
    read pages a real population: ``total`` is the number of subject rows matching the
    query and ``offset`` walks them, so all four shared fields mean exactly what they mean
    on every other v4 list. Redeclaring them in a standalone model would let those
    meanings drift with nothing to notice. Contrast :class:`SimilarVersesOut`, which
    *is* standalone precisely because its rows do not pre-exist and a ``total`` there
    would be a number that is present, defensible and misleading.

    **:class:`~api_v4.pagination.V4Page` itself is untouched**, which is the constraint
    that rules out the obvious alternative. It is the shared envelope for the eleven v4
    list endpoints that use it — versions, revisions, verses, both groups reads,
    assessments, results, ngrams, alignment-scores, missing-words and text-lengths — and a
    peer-ids field has no meaning on any of them.

    ``next_updated_since`` is inherited and stays null, for the reason ``/results`` gives:
    ``assessment_result`` carries no modification timestamp, so there is no watermark to
    publish and this list has no delta feed.
    """

    against_assessment_ids: list[int] = Field(
        description=(
            "The peer assessments this page was compared against, deduplicated and in "
            "the order `against` named them. Present because the path names only the "
            "subject: a response holding `mean_score` without saying what the mean is "
            "over cannot be interpreted on its own, and a client holding several of "
            "these needs to tell them apart. Shorter than the `against` you sent if you "
            "repeated an id — a peer named twice is one witness."
        ),
    )

    @classmethod
    def create(
        cls,
        *,
        items: list,
        total: int,
        pagination,
        against_assessment_ids: list[int],
        next_updated_since=None,
    ) -> "ScoreComparisonPage":
        """As :meth:`V4Page.create`, with the peer ids the envelope also carries.

        Overridden rather than inherited because the extra field is *required*: giving it
        a default so the parent's ``create`` could build this model would mean a page that
        forgot to name its peers would serialize as one compared against nobody. The four
        shared values still come off the ``pagination`` dependency rather than being
        copied by the caller, which is the drift the parent's ``create`` exists to prevent.
        """
        return cls(
            items=items,
            total=total,
            limit=pagination.limit,
            offset=pagination.offset,
            next_updated_since=next_updated_since,
            against_assessment_ids=against_assessment_ids,
        )


__all__ = [
    "BOOK_ABBREVIATION_LENGTH",
    "RESPONSE_LANGUAGE_MAX_LENGTH",
    "VREF_MAX_LENGTH",
    "AgentCritiqueOptions",
    "AlignmentScoreOut",
    "AlignmentScoreType",
    "AssessmentCreate",
    "AssessmentJob",
    "AssessmentOptions",
    "AssessmentOptionsBase",
    "AssessmentOut",
    "AssessmentResultAggregateOut",
    "AssessmentResultOut",
    "AssessmentResultRow",
    "MissingWordOut",
    "MissingWordTargetOut",
    "NgramResultOut",
    "NgramsOptions",
    "ReferencedAssessmentOptions",
    "ResultAggregate",
    "ResultScope",
    "ScoreComparisonAggregateOut",
    "ScoreComparisonOut",
    "ScoreComparisonPage",
    "ScoreComparisonRow",
    "SemanticSimilarityOptions",
    "SentenceLengthOptions",
    "SimilarVerseOut",
    "SimilarVersesOut",
    "TextLengthsAggregateOut",
    "TextLengthsOptions",
    "TextLengthsOut",
    "TextLengthsRow",
    "TfidfOptions",
    "VerseScope",
    "WordAlignmentOptions",
]
