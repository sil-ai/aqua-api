"""v4 Predict request/response schemas (issue #894, epic #842).

Four operations share this module: the fan-out (``POST /v4/predictions``), its poll
(``GET /v4/predictions/{job_id}``), and the two standalone comparisons
(``POST /v4/predictions/semantic-similarity``,
``POST /v4/predictions/length-comparison``).


The vocabulary decision: ``apps`` is the assessment-type vocabulary
------------------------------------------------------------------

v3 typed ``apps`` as a free-form ``List[str]`` and validated membership at runtime
(a ``400`` naming the unknown entries). v4 closes it into :class:`PredictApp`, per
#842's rule that filter invariants live in the request model so an impossible request
cannot be constructed — the valid names then reach ``/v4/openapi.json`` instead of
living in a route's error message.

Closing it forced a choice of spellings, because v3's keys mixed three conventions in
one list (``agent``, ``text_lengths``, ``word_alignment``, ``semantic-similarity``).
**v4 publishes the values** :class:`~schemas.assessment.AssessmentType` **already
publishes**, so "which analysis" is one word across the whole v4 surface: the string
that selects a predict app is the same string ``GET /v4/assessments?type=`` takes and
``AssessmentOut.type`` returns. Three of the six change spelling from v3
(``agent`` -> ``agent-critique``, ``text_lengths`` -> ``text-lengths``,
``word_alignment`` -> ``word-alignment``), and the keys of
:attr:`PredictOut.results` change with them.

That choice pays a second dividend, verified against the runner rather than assumed:
**each value is also the Modal app name it dispatches to.** v3 carried a
``PREDICT_APPS`` dict to translate its keys into app names; every value in that dict is
one of these six strings (checked against
``aqua-assessments/assessments/*/app.py``'s ``modal.App("...")`` literals), so v4 needs
no mapping table at all and cannot drift from one. ``predict_service`` dispatches on
``app.value``; a test pins the equality in both directions.

:class:`PredictApp` is **not** ``AssessmentType`` itself, and the difference is the one
value that would break it: ``sentence-length`` is an assessment type with no predict
app, so reusing the enum wholesale would publish a seventh app that 500s on dispatch.
The six are written out rather than computed for the reason this repo writes such sets
out generally — the test states the expected surface instead of re-deriving whatever
the code happens to do.


What the poll body carries, and the one thing it does not
---------------------------------------------------------

The poll merges the resource with the envelope rather than nesting it under
``result``, following ``GET /v4/assessments/{id}`` (#893's 2026-08-26 decision 1):
``includes`` and ``pairs`` sit at the top level beside ``job_id`` / ``state`` /
``error``, and ``result`` is null in every state. The alternative — a bare
:class:`~api_v4.jobs.JobEnvelope` with everything under ``result`` — would make a
client dig for fields it needs by name, and ``pairs`` is shaped data, not an opaque
outcome blob.

**``lexeme_cards`` is not carried.** v3's :class:`~schemas.predict.PredictJobPair` has
the field and its docstring calls this poll "the only surface where clients see"
newly-minted cards. Both halves of that are now out of date, independently:

* The lexeme-card resource was retired from v4 on 2026-09-10; PR #949 removed the whole
  ``/v4/lexeme-cards`` surface. Carrying the field here would re-admit a retired
  resource through a side door, with no v4 endpoint that can read or write the cards it
  names.
* On this path the field is always empty anyway. The poll exists only for a request
  that asked for translation, and the runner's agent ``predict()`` says of exactly that
  case: "When translating, ``lexeme_cards`` is ``[]`` — the ICL passes don't consult
  cards." The v3 docstring's claim predates the lean-ICL switchover.


Bounds
------

``max_length`` on the text fields and ``max_length`` on ``pairs`` are v3's, unchanged:
they bound what a single request can hand to a GPU container, and v3's values have held
in production. They are restated here rather than imported because
:mod:`schemas.predict` is frozen v3 and v4 owns its own request contract.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import Field, field_validator, model_validator

from api_v4.jobs import JobEnvelope, JobState
from api_v4.schemas.base import V4BaseModel

#: Longest single text v4 predict accepts, in characters. v3's cap on every one of
#: ``TextPair.source_text`` / ``target_text``, ``SemanticSimilarityRequest.text1`` /
#: ``text2`` and the two ``text-lengths`` query parameters — one number because it was
#: one number on v3, and because all five bound the same thing: one verse-sized string.
MAX_TEXT_CHARS = 10000

#: Longest ``vref`` label accepted. v3's cap. Deliberately generous for a verse
#: reference (``GEN 1:1`` is seven characters): the field is an opaque caller-supplied
#: label that predict echoes back and never parses, and merged spans push it longer.
MAX_VREF_CHARS = 50

#: Most pairs one fan-out may carry. v3's cap, which is roughly a long chapter's worth
#: of verses with room to spare.
MAX_PAIRS = 5000

#: Upper bound on ``limit``, the per-pair neighbour cap the ``tfidf`` app honours.
#: v3's cap.
MAX_NEIGHBOUR_LIMIT = 10000


class PredictApp(str, Enum):
    """The analyses ``POST /v4/predictions`` can fan a request out to.

    Values are :class:`~schemas.assessment.AssessmentType`'s, which are also the Modal
    app names — see the module docstring for why that is one decision rather than a
    coincidence, and for why this is a separate enum instead of ``AssessmentType``.
    """

    #: Trained n-gram matches for the target text, and for the source where a
    #: source-side corpus exists.
    ngrams = "ngrams"
    #: TF-IDF nearest-neighbour verses, per side, per pair.
    tfidf = "tfidf"
    #: The agent's back-translation and MQM critique. The only app with a slow leg —
    #: see :class:`PredictJobHandle`.
    agent_critique = "agent-critique"
    #: LaBSE cosine similarity between the two texts of each pair.
    semantic_similarity = "semantic-similarity"
    #: Word- and character-count differences per pair.
    text_lengths = "text-lengths"
    #: Eflomal alignment scores for each pair, against trained artifacts.
    word_alignment = "word-alignment"


class PredictInclude(str, Enum):
    """The two slow agent passes a caller can request, echoed back on the job.

    ``critique`` requires ``translation`` — it runs over the translation — which
    :class:`PredictRequest` enforces so the pair cannot be requested the wrong way
    round.
    """

    translation = "translation"
    critique = "critique"


class PredictAppStatus(str, Enum):
    """How one app's leg of the fan-out ended.

    Three values, not two, because "this app has never been trained for this revision"
    is an actionable state a caller can fix, not a failure: v3 separates it and v4
    keeps the distinction. The fan-out isolates per-app failure, so one app reporting
    ``error`` says nothing about the others — which is why this is per-app rather than
    a status on the response as a whole.
    """

    #: The app returned a result; ``data`` carries it.
    ok = "ok"
    #: The app raised or timed out; ``error`` carries a short reason.
    error = "error"
    #: The app needs a finished training run for this revision and has none.
    not_trained = "not_trained"


class PredictPair(V4BaseModel):
    """One source/target text pair to analyse.

    ``vref`` is an optional label: predict never parses it and never requires it to
    name a real verse — it is echoed back so a caller can match results to inputs.
    Callers that omit it match by index instead, which is why every response echoes the
    submitted pairs in submission order.

    The agent app is the one exception to ``vref`` being optional *in practice*: its
    translation and critique passes need ``vref`` and ``source_text`` on every pair and
    report a per-app ``error`` without them. That is the runner's rule, enforced where
    it is known rather than here, since the same pair is legal for the other five apps.
    """

    vref: str | None = Field(
        default=None,
        max_length=MAX_VREF_CHARS,
        description=(
            "Optional label for this pair, conventionally a verse reference "
            "(`GEN 1:1`). Echoed back untouched; never parsed."
        ),
    )
    source_text: str | None = Field(
        default=None,
        max_length=MAX_TEXT_CHARS,
        description=(
            "The reference-language text. Optional here because three of the six apps "
            "do not read it; the ones that do report a per-app error when it is absent."
        ),
    )
    target_text: str = Field(
        max_length=MAX_TEXT_CHARS,
        description="The text being assessed. Required by every app.",
    )


class PredictRequest(V4BaseModel):
    """Request body for ``POST /v4/predictions``.

    Closed allowlist (``extra="forbid"``), as on every v4 request body.

    **The five selector ids are all optional and all authorized.** Each app reads the
    subset it needs (the runner's per-app ``predict()`` docstrings state which), so
    which ids a caller sends depends on which apps they select — but any id they *do*
    send must name something they can see, or the request is a ``404``. v3 checked only
    three of the five, leaving ``source_version_id`` and ``target_version_id``
    unchecked; see :mod:`predict_routes.v4.predict_service`.
    """

    pairs: list[PredictPair] = Field(
        min_length=1,
        max_length=MAX_PAIRS,
        description="The pairs to analyse, in the order results are returned in.",
    )
    apps: list[PredictApp] | None = Field(
        default=None,
        min_length=1,
        description=(
            "Which analyses to run. Omit to run all six. Repeats are collapsed, "
            "order-preserving, and an unknown name is a 422 rather than a partial run."
        ),
    )
    assessment_id: int | None = Field(
        default=None,
        description=(
            "Resolve trained artifacts from this assessment. Read by `tfidf` and "
            "`word-alignment`; the caller must be able to see it."
        ),
    )
    revision_id: int | None = Field(
        default=None,
        description=(
            "The revision the target text belongs to, used to find its trained "
            "artifacts. The caller must be able to see it."
        ),
    )
    reference_id: int | None = Field(
        default=None,
        description=(
            "The revision the source text belongs to. The caller must be able to see "
            "it."
        ),
    )
    source_version_id: int | None = Field(
        default=None,
        description=(
            "Version of the source text. The caller must be able to see it. Note this "
            "is a **version** id, not a revision id."
        ),
    )
    target_version_id: int | None = Field(
        default=None,
        description=(
            "Version of the target text. The caller must be able to see it. Note this "
            "is a **version** id, not a revision id."
        ),
    )
    limit: int | None = Field(
        default=None,
        ge=1,
        le=MAX_NEIGHBOUR_LIMIT,
        description=(
            "Cap on neighbours returned per side per pair. Honoured by `tfidf` only, "
            "which defaults to 10 when this is omitted."
        ),
    )
    include_translation: bool = Field(
        default=True,
        description=(
            "Run the agent's back-translation pass. This is the slow leg: when it is "
            "on and `agent-critique` is selected, the response carries a `job` to poll "
            "and the inline `agent-critique` result holds the fast slice only."
        ),
    )
    include_critique: bool = Field(
        default=True,
        description=(
            "Run the agent's MQM critique pass, which runs over the translation and so "
            "requires `include_translation`. Sending it as `true` alongside "
            "`include_translation: false` is a 422; leaving it unset while turning "
            "translation off means 'fast path only' and turns it off too."
        ),
    )
    bt_pivot: bool | None = Field(
        default=None,
        description=(
            "Route the agent's back-translation through a pivot language (render the "
            "target into a close language first, then into the reference language) "
            "instead of going direct. Null — the default — leaves the choice to the "
            "agent's deployment default. Whether pivoting reads better varies by "
            "language pair, so it is settable per request rather than fixed at deploy "
            "time (see #911)."
        ),
    )

    model_config = {
        **V4BaseModel.model_config,
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "pairs": [
                    {
                        "vref": "GEN 1:1",
                        "source_text": (
                            "In the beginning God created the heavens and the earth."
                        ),
                        "target_text": "Hapo mwanzo Mungu aliumba mbingu na dunia.",
                    }
                ],
                "apps": ["ngrams", "tfidf", "agent-critique"],
                "revision_id": 1,
                "reference_id": 2,
                "source_version_id": 1,
                "target_version_id": 2,
                "include_translation": True,
                "include_critique": True,
            }
        },
    }

    @field_validator("apps")
    @classmethod
    def _dedupe(cls, value: list[PredictApp] | None) -> list[PredictApp] | None:
        """Collapse repeats, order-preserving.

        A repeated app is a caller slip, not a request to run it twice — and running it
        twice is what the un-deduplicated list would do, at full GPU cost, before the
        second result overwrote the first in a dict keyed by app. v3 deduplicates in the
        handler; doing it here means the dispatched set and the validated set are the
        same object.
        """
        if value is None:
            return None
        return list(dict.fromkeys(value))

    @model_validator(mode="after")
    def _critique_requires_translation(self) -> PredictRequest:
        """Reject critique-without-translation, but only where it was actually asked for.

        Mirrors the agent-side validator in ``aqua-assessments``
        (``shared/predict_input.py``): critique runs over translations, so asking for it
        without translation is a bug, not a silent no-op. Rejecting at the API boundary
        means the caller sees a 422 rather than a per-app error string buried in the
        fan-out response.

        Both flags default to ``True``, so a caller opting out of translation without
        mentioning critique means "fast path only" — turn critique off with it rather
        than 422ing on a default they never set. (The assignment adds
        ``include_critique`` to ``model_fields_set``, so after validation that set no
        longer reflects only caller-sent fields.)
        """
        if self.include_critique and not self.include_translation:
            if "include_critique" not in self.model_fields_set:
                self.include_critique = False
            else:
                raise ValueError(
                    "include_critique=True requires include_translation=True"
                )
        return self


class PredictAppResult(V4BaseModel):
    """One app's leg of the fan-out.

    ``data`` is deliberately untyped, and the obvious objection is that
    :class:`PredictJobPair` — in this same module, over the same agent — *is* typed. The
    two are not the same payload, which is the answer:

    * The poll's payload is two fields of one agent pair (``translation`` and
      ``critique``), written by one line of the runner.
    * The fan-out's ``agent-critique`` payload is the agent's whole ``predict()`` reply:
      a grammar sketch, two language profiles, and per-pair entries that still carry the
      ``lexeme_cards`` v4 retired. It is also always the *fast slice* when a slow leg was
      spawned, so its ``translation`` and ``critique`` are null by construction. Typing
      it would publish a retired field and a shape that is mostly holes.
    * The other five are unrelated and large — n-gram corpus blocks, neighbour lists
      carrying each verse's text in two revisions, alignment links, missing-word
      tallies.

    All six are the runner's contract, versioned in ``aqua-assessments`` rather than
    here, so typing them would put this repo's release cycle in front of theirs. A
    discriminated union keyed by :class:`PredictApp` is the shape that would do it
    properly, and it is six schema trees of work that belongs with whoever needs one of
    them typed. The per-app shapes are documented where they are produced, in each app's
    ``predict()`` docstring.
    """

    status: PredictAppStatus = Field(
        description="How this app's leg ended. Branch on this before reading `data`.",
    )
    data: Any | None = Field(
        default=None,
        description=(
            "The app's own result shape, present when `status` is `ok`. See the app's "
            "`predict()` docstring in aqua-assessments for the per-app shape."
        ),
    )
    error: str | None = Field(
        default=None,
        description=(
            "Short reason, present when `status` is `error` or `not_trained`. Prose "
            "for a human reading a failed leg — not a stable code to branch on."
        ),
    )
    duration_ms: int = Field(
        description=(
            "Wall-clock time this leg took, including Modal container start. Reported "
            "for every status, so a timeout can be told from an immediate refusal."
        ),
    )


class PredictJobHandle(V4BaseModel):
    """The slow agent leg, handed back inside the fan-out's ``200``.

    Null when no slow leg was started — which is the common case: it takes both
    ``agent-critique`` among the selected apps *and* one of the two ``include_`` flags.
    The key is always present (``"job": null``), never omitted; v3 dropped it from the
    body when unset, and #842's envelope rule is that every key is present on every
    response so a client can read one shape.

    **Why this carries a ``poll_url`` when the rest of v4 uses a ``Location`` header.**
    :mod:`api_v4.jobs` puts the poll URL in ``Location`` on a ``202``, and deliberately
    not in the body. That applies to a submit, whose whole response is about the job.
    This response is not a submit: it is a ``200`` carrying six apps' results, one of
    which happens to have a slow leg (#894's "one response shape, always 200"). A
    ``Location`` on a ``200`` means "this body is a representation of that URL", which
    would be false here. So the URL travels as a field, and it names the concrete
    ``/v4`` path rather than the floating ``/latest`` alias v3 emitted — a client that
    called v4 is handed v4.
    """

    job_id: str = Field(
        description=(
            "Identifier of the spawned job, and the id segment of `poll_url`. Opaque; "
            "matches the `job_id` the poll reports."
        ),
    )
    state: JobState = Field(
        description=(
            "The job's state at the moment the fan-out answered — `RUNNING` normally, "
            "`FAILED` when the spawn itself could not be placed."
        ),
    )
    includes: list[PredictInclude] = Field(
        description=(
            "Which slow passes this job runs, echoed from the request so a client that "
            "sent defaults knows what it asked for."
        ),
    )
    poll_url: str = Field(
        description=(
            "Where to poll for the slow result — a root-relative `/v4` path. Derived "
            "from the poll route itself, so it cannot drift from the route it names."
        ),
    )
    retry_after_s: int = Field(
        description=(
            "How long to wait before the first poll, in seconds. The poll re-advertises "
            "it as a `Retry-After` header on every non-terminal response; it is "
            "repeated here because this `200` cannot carry that header meaningfully, "
            "and without it a client would have to invent its own first cadence."
        ),
    )


class PredictOut(V4BaseModel):
    """The ``200`` body of ``POST /v4/predictions``.

    One shape, always ``200`` — #894's decision, reaffirming v3's. The slow agent leg
    rides as a nullable :attr:`job` rather than turning the endpoint into a ``202``,
    because two response shapes on one path is a cost every caller pays so that one
    optional leg can be signalled in the status line.
    """

    pairs: list[PredictPair] = Field(
        description=(
            "The submitted pairs, echoed in submission order. Every app's `data` "
            "reports its own results in this same order, so a caller that omitted "
            "`vref` can still match by index."
        ),
    )
    results: dict[PredictApp, PredictAppResult] = Field(
        description=(
            "One entry per selected app, keyed by app name. Per-app failure is "
            "isolated: a missing or failed app never suppresses the others, so every "
            "selected app has an entry whatever happened to it."
        ),
    )
    job: PredictJobHandle | None = Field(
        default=None,
        description=(
            "The slow agent leg, or null when none was started. Always present as a "
            "key."
        ),
    )


class PredictTranslation(V4BaseModel):
    """The agent's back-translations of one pair.

    Three renderings of the same verse, from the same pass: ``hyper_literal`` tracks the
    target's morphology, ``literal`` reads as a sentence, and ``english_translation`` is
    the reference-language rendering. Where the critique's verify pass corrected the
    back-translation, ``literal`` is the *corrected* reading and the original rides
    ``critique.bt_correction``.

    Typed — unlike v3, which left this an untyped ``Dict[str, Any]`` — because the three
    keys are written unconditionally by one line of the runner and have been stable
    across the ICL switchover. ``extra="allow"`` keeps a fourth rendering visible to
    clients the day it appears rather than dropping it, and every field is optional, so
    an agent that *stops* emitting one cannot turn a poll into a 500.

    The residual risk, stated rather than papered over: ``extra="allow"`` and optionality
    cover a missing or added key, not a declared key arriving with the wrong *type*. A
    runner that emitted a non-string ``literal`` would 500 every poll of that job, where
    v3's untyped dict would have passed it through. That is accepted, and it is the same
    posture :class:`PredictCritiqueIssue` already takes over the same stored data — it
    enforces ``dimension: str`` while deliberately refusing to enforce the *constraints*
    (the ``Literal``, the 1-5 bound, the length caps). Types are enforced on this read
    path; value constraints are advertised and not enforced. The line is there because a
    type is what a client's own parser will assume anyway, while a constraint is what
    the runner is most likely to widen without telling us.
    """

    hyper_literal: str | None = Field(
        default=None,
        description="Morpheme-tracking rendering; reads awkwardly by design.",
    )
    literal: str | None = Field(
        default=None,
        description=(
            "Readable back-translation. The corrected reading where the critique's "
            "verify pass revised it."
        ),
    )
    english_translation: str | None = Field(
        default=None,
        description="Rendering into the reference language.",
    )

    model_config = {**V4BaseModel.model_config, "extra": "allow"}


class PredictCritiqueIssue(V4BaseModel):
    """One MQM-aligned issue the agent raised against a pair's back-translation.

    Mirrors the per-issue shape the agent emits. Extra fields the agent may add are
    preserved on the wire via ``extra="allow"`` rather than dropped, so consumers can
    rely on the documented fields while new agent attributes still reach them.

    **Constraints are advertised, not enforced.** ``dimension`` is a plain ``str``, not
    a ``Literal``, and ``subtype`` / ``detector`` / ``severity`` carry no length or
    range bounds. Validation here runs over data the agent already wrote into a stored
    job result, so extra-strict typing would convert a previously-``200`` response into
    a ``500`` the first time the agent emits an unexpected value — a new MQM dimension
    added runner-side before aqua-api, a legacy row predating a tightened constraint.
    The documented values live in each field's ``description`` and in
    ``json_schema_extra``, so Swagger UI still shows them; callers should match
    case-insensitively or by prefix.
    """

    dimension: str = Field(
        description=(
            "MQM dimension. Documented values: 'accuracy', 'terminology', "
            "'linguistic_conventions'. Typed as a plain string on the read path so an "
            "unexpected agent value cannot 500 the poll."
        ),
        json_schema_extra={
            "enum": ["accuracy", "terminology", "linguistic_conventions"],
        },
    )
    subtype: str = Field(
        description=(
            "Free-form MQM subtype, e.g. 'omission', 'addition', 'mistranslation', "
            "'mistranslation/hallucination-numbers'. Not an enum — match "
            "case-insensitively or by prefix."
        ),
        json_schema_extra={"maxLength": 100},
    )
    source_text: str | None = Field(
        default=None, description="The source snippet the issue is about."
    )
    draft_text: str | None = Field(
        default=None, description="The drafted text the issue objects to."
    )
    comments: str | None = Field(
        default=None, description="The agent's prose explanation."
    )
    severity: int | None = Field(
        default=None,
        description=(
            "Severity the agent assigned, typically 1-5; null when the model omitted "
            "it. The range is advertised but not enforced (see the class docstring)."
        ),
        json_schema_extra={"minimum": 1, "maximum": 5},
    )
    detector: str | None = Field(
        default=None,
        description=(
            "Tag identifying the automated detector that flagged the issue, e.g. "
            "'number_diff'. Null for issues the model raised on its own."
        ),
        json_schema_extra={"maxLength": 50},
    )
    evidence: list[str] | None = Field(
        default=None,
        description="Supporting snippets the detector or model attached.",
    )

    model_config = {**V4BaseModel.model_config, "extra": "allow"}


class PredictCritique(V4BaseModel):
    """The critique payload for one pair.

    ``issues`` is the canonical MQM list (#793) and the only documented key, matching
    v3. The runner also emits ``omissions``, ``additions`` and ``replacements`` (the
    pre-MQM lists it kept for compatibility) and ``bt_correction`` (an audit record of a
    verify-pass correction to the back-translation, null when the reading stood). Those
    four reach clients through ``extra="allow"`` rather than being declared, because
    their element shapes are the runner's to change and this read must not 500 when they
    do — the same call v3 made. They are named here so a reader knows what arrives.
    """

    issues: list[PredictCritiqueIssue] = Field(
        default_factory=list,
        description="The MQM issues raised against this pair, or an empty list.",
    )

    model_config = {
        **V4BaseModel.model_config,
        "extra": "allow",
        "json_schema_extra": {
            "example": {
                "issues": [
                    {
                        "dimension": "accuracy",
                        "subtype": "mistranslation/hallucination-numbers",
                        "source_text": "forty days",
                        "draft_text": "fourteen days",
                        "comments": "Number mistranslated",
                        "severity": 4,
                        "detector": "number_diff",
                        "evidence": ["source: 40", "draft: 14"],
                    }
                ]
            }
        },
    }


class PredictJobPair(V4BaseModel):
    """One pair's slow-path result, on the poll.

    The ``vref`` / ``source_text`` / ``target_text`` echo always comes from the
    *submitted* pair rather than from the agent's response, so a caller that omitted
    ``vref`` can match by index and a runner-side bug that mangled the echo cannot
    propagate into it. ``translation`` and ``critique`` are positional from the agent's
    reply, which preserves input order.

    Both are null until the job reaches ``SUCCEEDED``, and ``critique`` stays null for a
    job that asked for translation only — so a client reads ``includes`` to know which
    it should expect. See the module docstring for why there is no ``lexeme_cards``.
    """

    vref: str | None = Field(
        default=None, description="The submitted label for this pair, echoed back."
    )
    source_text: str | None = Field(
        default=None, description="The submitted source text, echoed back."
    )
    target_text: str = Field(description="The submitted target text, echoed back.")
    translation: PredictTranslation | None = Field(
        default=None,
        description=(
            "The agent's back-translations, once the job succeeds. Null while it runs, "
            "and null for a job that did not ask for translation."
        ),
    )
    critique: PredictCritique | None = Field(
        default=None,
        description=(
            "The agent's MQM critique, once the job succeeds. Null while it runs, and "
            "null for a job that asked for translation only."
        ),
    )


class PredictJobOut(V4BaseModel):
    """A predict job as a resource: what was asked for, and what came back per pair."""

    includes: list[PredictInclude] = Field(
        description=(
            "Which slow passes this job runs, recorded at submission. Tells a client "
            "whether a null `critique` means 'not asked for' or 'not finished'."
        ),
    )
    pairs: list[PredictJobPair] = Field(
        description=(
            "One entry per submitted pair, in submission order, whatever the job's "
            "state — so a client polling a running job still sees what it submitted."
        ),
    )


class PredictJob(PredictJobOut, JobEnvelope):
    """The poll body for ``GET /v4/predictions/{job_id}``: the resource *plus* the envelope.

    Adds :class:`~api_v4.jobs.JobEnvelope`'s ``job_id`` / ``state`` / ``result`` /
    ``error`` to :class:`PredictJobOut`, so one poll answers both "what is this job
    doing" and "what did it produce". The same merge
    ``GET /v4/assessments/{id}`` makes, for the same reason — and all four envelope keys
    are present on every response, ``"error": null`` included, so the poll route must
    **not** carry ``response_model_exclude_none=True``.

    Inheriting the envelope rather than restating it keeps its invariants enforced here:
    ``error`` is non-null exactly when ``state`` is ``FAILED``, and ``result`` is null
    unless ``state`` is ``SUCCEEDED``. ``result`` is inherited untyped and is null in
    every state, which the envelope explicitly permits for a ``SUCCEEDED`` job: the
    outcome is published as ``pairs``, which is shaped data a client reads by field.

    ``PENDING`` never appears. ``predict_jobs.status`` has three values and none of them
    means "queued" — a row is inserted already ``running`` (the Modal spawn happens
    first) or already ``failed`` (the spawn threw), so there is no state in which this
    poll can answer ``202``.
    """


class SimilarityRequest(V4BaseModel):
    """Request body for ``POST /v4/predictions/semantic-similarity``.

    Closed allowlist (``extra="forbid"``). The two version ids are required, as on v3,
    and both must name a version the caller can see — the authorization v3 never
    performed (#861).

    The texts are named ``source_text`` / ``target_text`` rather than v3's ``text1`` /
    ``text2``, matching :class:`PredictPair` and the rest of the surface: the pair is
    ordered, the version ids are already named for the two roles, and ``text1`` said
    nothing about which role it played.
    """

    source_text: str = Field(
        max_length=MAX_TEXT_CHARS,
        description="The reference-language text, belonging to `source_version_id`.",
    )
    target_text: str = Field(
        max_length=MAX_TEXT_CHARS,
        description="The assessed text, belonging to `target_version_id`.",
    )
    source_version_id: int = Field(
        description=(
            "Version of the source text. With `target_version_id` it selects the "
            "fine-tuned model; the caller must be able to see it."
        ),
    )
    target_version_id: int = Field(
        description=(
            "Version of the target text. With `source_version_id` it selects the "
            "fine-tuned model; the caller must be able to see it."
        ),
    )

    model_config = {
        **V4BaseModel.model_config,
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "source_text": (
                    "In the beginning God created the heavens and the earth."
                ),
                "target_text": "Hapo mwanzo Mungu aliumba mbingu na dunia.",
                "source_version_id": 1,
                "target_version_id": 2,
            }
        },
    }


class SimilarityOut(V4BaseModel):
    """The ``200`` body of ``POST /v4/predictions/semantic-similarity``."""

    score: float = Field(
        description=(
            "Cosine similarity between the two texts' embeddings, from the model "
            "fine-tuned for this version pair. Higher is more similar. Not bounded to "
            "[0, 1] here: the bound is a property of the embedding space, not of this "
            "contract."
        ),
    )


class LengthComparisonRequest(V4BaseModel):
    """Request body for ``POST /v4/predictions/length-comparison``.

    Closed allowlist (``extra="forbid"``). A ``POST`` with a JSON body rather than v3's
    ``GET`` with two 10,000-character query parameters: verse-sized text in a query
    string is what the move to JSON bodies (#826) exists to end, and two of them can
    exceed what intermediaries will carry.

    **No ids, and nothing to authorize beyond the caller's token.** This is a pure
    string comparison — it opens no database session and reads no resource — so there is
    no id to check ownership of. #861 named v3's ``GET /predict/text-lengths`` as
    lacking resource authorization; that half of the finding does not apply, on v3 or
    here, because there is no resource in the request.
    """

    source_text: str = Field(
        max_length=MAX_TEXT_CHARS,
        description="The reference-language text.",
    )
    target_text: str = Field(
        max_length=MAX_TEXT_CHARS,
        description="The assessed text.",
    )

    model_config = {
        **V4BaseModel.model_config,
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "source_text": (
                    "In the beginning God created the heavens and the earth."
                ),
                "target_text": "Hapo mwanzo Mungu aliumba mbingu na dunia.",
            }
        },
    }


class LengthComparisonOut(V4BaseModel):
    """The ``200`` body of ``POST /v4/predictions/length-comparison``.

    Both differences are **source minus target**, so a positive value means the source
    is longer — v3's sign convention, and the runner's ``text-lengths`` app's.
    """

    word_count_difference: int = Field(
        description=(
            "Whitespace-delimited word count of `source_text` minus that of "
            "`target_text`. A blank or whitespace-only text counts as zero words."
        ),
    )
    char_count_difference: int = Field(
        description=(
            "Character count of `source_text` minus that of `target_text`, counted "
            "before any trimming."
        ),
    )


__all__ = [
    "MAX_NEIGHBOUR_LIMIT",
    "MAX_PAIRS",
    "MAX_TEXT_CHARS",
    "MAX_VREF_CHARS",
    "LengthComparisonOut",
    "LengthComparisonRequest",
    "PredictApp",
    "PredictAppResult",
    "PredictAppStatus",
    "PredictCritique",
    "PredictCritiqueIssue",
    "PredictInclude",
    "PredictJob",
    "PredictJobHandle",
    "PredictJobOut",
    "PredictJobPair",
    "PredictOut",
    "PredictPair",
    "PredictRequest",
    "PredictTranslation",
    "SimilarityOut",
    "SimilarityRequest",
]
