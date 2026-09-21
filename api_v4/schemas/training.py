"""v4 Training request/response schemas (issue #895, epic #842).

Six operations share this module: the submit (``POST /v4/training-sessions``), the
session read and its results (``GET /v4/training-sessions/{session_id}``,
``.../results``), and the three job operations (``GET /v4/training-jobs``,
``GET /v4/training-jobs/{job_id}``, ``DELETE /v4/training-jobs/{job_id}``).


A session is a key, not a table
--------------------------------

``training_job.session_id`` is a nullable ``Text`` column with an index — there is no
``training_session`` row anywhere. :class:`TrainingSessionOut` is therefore a *derived*
view over the jobs sharing a key: it has no created-at of its own, no owner of its own,
and no metadata a job does not carry. Two consequences are visible on the wire and are
deliberate:

* A session with no jobs is indistinguishable from one that never existed, so the read
  is a ``404`` rather than a ``200`` with an empty list.
* Everything the session reports is computed from its jobs each time it is read.


``state`` on a session, and why it is nullable
-----------------------------------------------

A submit fans out to as many as five jobs, each with its own state, but
:class:`~api_v4.jobs.JobEnvelope` has exactly one ``state``. A session is not a job, so
this module does not pretend it is one: :class:`TrainingSessionOut` carries **both** an
aggregate ``state`` to branch on and the per-job list to drill into. The aggregate is
defined, not inferred:

* ``FAILED`` if any job failed;
* otherwise ``RUNNING`` if any job is non-terminal;
* otherwise ``SUCCEEDED``.

So a session reports ``RUNNING`` from the moment it is created, even before any runner
picks a job up — it never reports ``PENDING``, and the session read therefore never
answers ``202``. The per-job ``state`` is where "queued but not started" is visible.

``null`` is the fourth answer, and it is the one case the three rules above cannot
cover: see the next section.


``training_job.assessment_id`` is nullable, and this module does not launder it
-------------------------------------------------------------------------------

``TrainingJob`` stores no state at all — status, timing and progress live on the linked
``Assessment`` row (aqua-api#584/#593) — and the foreign key is ``ON DELETE SET NULL``.
A training row can therefore exist with **no state carrier**. That is a data-integrity
fault rather than a job state, and :mod:`api_v4.jobs` deliberately offers no ``UNKNOWN``
member to hide it in, so the three shapes here answer it three different ways for three
different reasons:

* :class:`TrainingJobOut` (the list row, and the session's per-job entry) reports
  ``state: null`` and names the fault in ``error``. A list that silently dropped such a
  row would hide the fault from the only view that could surface it.
* :class:`TrainingJobDetail` (the single-job poll) **cannot** do that: it is a
  :class:`~api_v4.jobs.JobEnvelope`, whose validator requires a state and forbids an
  ``error`` on anything but ``FAILED``. So the poll raises
  ``TRAINING_JOB_STATE_UNAVAILABLE`` instead of returning a body.
* :class:`TrainingSessionOut` reports ``state: null`` for the whole session when any of
  its jobs cannot report one, because an aggregate computed over a job whose outcome is
  unknown would be a guess.

``error`` accordingly has one meaning across every shape here — *what is wrong with this
job* — and exactly two causes: the job failed, or its state carrier is missing. Both
arrive as the same ``{code, message, details}`` object the v4 error envelope uses, so a
client parses one error shape wherever it appears.


The dual-id observability, stated once
---------------------------------------

A training run is visible under **two** ids: its ``training_job.id`` and the
``assessment.id`` it links to. Both report the same state, because the state is read
from the assessment either way. They are one job seen twice, not two jobs.
``assessment_id`` is published on :class:`TrainingJobOut` because it is the key every
result row in the database is stored under — but it is **not** a job id on this surface
and no v4 training endpoint accepts one. (Nor does the assessments surface serve it:
``GET /v4/assessments`` excludes ``is_training`` rows.)


What the results row is
------------------------

:class:`TrainingResultRow` keeps v3's interleaved shape — one row per verse, carrying
each trained type's output for that verse — rather than splitting per type. The vref
universe is the union across every finished type in the session, which is what makes one
offset-paginated sequence coherent; splitting would make four pages that have to be
re-joined by the client. The per-type blocks mirror predict's per-pair shapes, so a
client can share a parser between the two surfaces.

``lexeme_cards`` is **not** carried. v3's ``TrainingSessionVrefResults`` has the field;
the lexeme-card resource was retired from v4 on 2026-09-10 and PR #949 removed the whole
``/v4/lexeme-cards`` surface, and the predict slice dropped the same field from its poll
for the same reason. Carrying it here would re-admit a retired resource through a third
door.
"""

from datetime import datetime
from typing import Any

from pydantic import Field, model_validator

from api_v4.errors import V4ErrorDetail
from api_v4.jobs import JobEnvelope, JobState
from api_v4.schemas.base import V4BaseModel
from schemas.training import TrainingType

#: Default neighbours per side, per verse, for the ``tfidf`` block of a results row.
#: v3's default, unchanged.
TFIDF_TOP_K_DEFAULT = 5
#: Ceiling on the same. v3's, unchanged: this multiplies the size of every row on the
#: page, so it is bounded far below the similarity read's own ``limit`` (100), which
#: bounds a single ranking rather than a ranking per row.
TFIDF_TOP_K_MAX = 50


class TrainingSessionCreate(V4BaseModel):
    """Request body for ``POST /v4/training-sessions``.

    Closed allowlist (``extra="forbid"``), as on every v4 request body.

    Each side of the pair is named **either** by version (the latest non-deleted
    revision is resolved server-side) **or** by revision (when a specific one is
    required), and exactly one of the two per side — the same rule v3's
    ``TrainingJobIn`` enforces, kept because the alternative is a request whose meaning
    depends on which field the server happens to read first.
    """

    source_version_id: int | None = Field(
        default=None,
        description=(
            "Train against the latest non-deleted revision of this version. Mutually "
            "exclusive with `source_revision_id`; exactly one of the two is required."
        ),
    )
    source_revision_id: int | None = Field(
        default=None,
        description=(
            "Train against this exact revision. Mutually exclusive with "
            "`source_version_id`; exactly one of the two is required."
        ),
    )
    target_version_id: int | None = Field(
        default=None,
        description=(
            "Train the latest non-deleted revision of this version. Mutually exclusive "
            "with `target_revision_id`; exactly one of the two is required."
        ),
    )
    target_revision_id: int | None = Field(
        default=None,
        description=(
            "Train this exact revision. Mutually exclusive with `target_version_id`; "
            "exactly one of the two is required."
        ),
    )
    apps: list[TrainingType] | None = Field(
        default=None,
        min_length=1,
        description=(
            "Which analyses to train. Omit it to train all of them. Each selected app "
            "becomes its own job in the session, and an app that already has an active "
            "job for this pair with these options is skipped rather than duplicated."
        ),
    )
    options: dict[str, Any] | None = Field(
        default=None,
        description=(
            "Per-app training options, passed through to every selected app — the "
            "runner reads the keys it knows and ignores the rest. An open object rather "
            "than a typed union, because one options object serves every app in the "
            "session; `POST /v4/assessments` types its options because one submit there "
            "runs exactly one type."
        ),
    )

    model_config = {
        **V4BaseModel.model_config,
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "source_version_id": 1,
                "target_version_id": 2,
                "apps": ["word-alignment", "tfidf"],
                "options": {"use_eflomal": True},
            }
        },
    }

    @model_validator(mode="after")
    def _one_id_per_side(self) -> "TrainingSessionCreate":
        if (self.source_version_id is None) == (self.source_revision_id is None):
            raise ValueError(
                "provide exactly one of source_version_id or source_revision_id"
            )
        if (self.target_version_id is None) == (self.target_revision_id is None):
            raise ValueError(
                "provide exactly one of target_version_id or target_revision_id"
            )
        return self


class TrainingJobOut(V4BaseModel):
    """One training job: a row of ``GET /v4/training-jobs``, and a session's job entry.

    Built from named columns, never by splatting the ORM row — the rule the Revisions
    slice states (#891), so the field set stays closed.

    ``state`` is nullable here and non-null on :class:`TrainingJobDetail`; the module
    docstring has the reason, and it is the one field whose optionality differs between
    the two shapes.
    """

    id: int = Field(
        description=(
            "The job's id — an integer, and the same value the single-job read reports "
            "as the string `job_id`."
        ),
    )
    session_id: str | None = Field(
        default=None,
        description=(
            "The submit that created this job. Every job created through v4 has one; "
            "the column is nullable and a legacy row may not."
        ),
    )
    type: TrainingType = Field(
        description="Which analysis this job trains.",
    )
    state: JobState | None = Field(
        default=None,
        description=(
            "The job's current public state; branch on this. Read from the linked "
            "assessment, since `training_job` stores no state of its own. **Null means "
            "the state could not be read** — the job has no linked assessment — which is "
            "a data-integrity fault, not a state: `error` names it. Null is rare enough "
            "that it is worth handling explicitly rather than treating as pending."
        ),
    )
    error: V4ErrorDetail | None = Field(
        default=None,
        description=(
            "What is wrong with this job, or null. Two causes, told apart by `code`: "
            "`JOB_FAILED` when the run failed (`state` is FAILED and the message is what "
            "the runner reported), and `TRAINING_JOB_STATE_UNAVAILABLE` when the job has "
            "no linked assessment (`state` is null). The same {code, message, details} "
            "object the v4 error envelope uses."
        ),
    )
    status_detail: str | None = Field(
        default=None,
        description=(
            "Free-text detail the runner last reported — a progress note while running, "
            "or the reason it failed. Prose for a human; do not parse it. On a failed "
            "job the same text is also the error's `message`."
        ),
    )
    percent_complete: float | None = Field(
        default=None,
        description=(
            "How far along the run is, 0-100, as last reported by the runner. Null when "
            "it has never reported progress, and null whenever `state` is."
        ),
    )
    source_revision_id: int = Field(
        description="The reference side of the pair this job trained on.",
    )
    target_revision_id: int = Field(
        description="The assessed side of the pair this job trained on.",
    )
    source_version_id: int = Field(
        description="Version of `source_revision_id`, denormalized onto the job.",
    )
    target_version_id: int = Field(
        description="Version of `target_revision_id`, denormalized onto the job.",
    )
    options: dict | None = Field(
        default=None,
        description=(
            "The options this job was created with, as stored. Not always what was "
            "submitted: `semantic-similarity` is stored with `finetune` forced on, which "
            "is what makes it a training run rather than an assessment."
        ),
    )
    assessment_id: int | None = Field(
        default=None,
        description=(
            "The assessment row carrying this job's state, timing and results. **Not a "
            "job id on this surface** — no v4 training endpoint accepts one, and "
            "`GET /v4/assessments` does not serve training rows. It is published because "
            "it is the key the stored result rows hang off. Null on a job whose "
            "assessment was deleted, which is what leaves `state` null."
        ),
    )
    requested_at: datetime | None = Field(
        default=None,
        description="When the job was submitted.",
    )
    started_at: datetime | None = Field(
        default=None,
        description="When the runner started work, or null if it has not yet.",
    )
    ended_at: datetime | None = Field(
        default=None,
        description="When the run reached a terminal state, or null if it has not.",
    )
    owner_id: int | None = Field(
        default=None,
        description=(
            "Id of the user who submitted the job. Null on rows created before the "
            "column existed — which is also why only an admin can delete those."
        ),
    )


class TrainingJobDetail(TrainingJobOut, JobEnvelope):
    """The body of ``GET /v4/training-jobs/{job_id}``: the job *plus* the envelope.

    Adds :class:`~api_v4.jobs.JobEnvelope`'s ``job_id`` and ``result`` to
    :class:`TrainingJobOut`.

    **Both fields the two bases share are re-declared below, and neither is optional to
    re-declare.** ``state`` and ``error`` mean narrower things here than on the list row,
    and Pydantic resolves a field it finds on two bases from whichever is listed *first*
    — ``TrainingJobOut`` — so an inherited field would silently publish the list row's
    wording in this model's schema. That is not a theoretical risk: it shipped in review.
    A field added to both bases later needs the same treatment.

    All four envelope keys are always present, ``"error": null`` included, so the read
    must **not** carry ``response_model_exclude_none=True``. The envelope's
    ``model_validator`` is inherited and still runs, so ``error`` is non-null exactly when
    ``state`` is ``FAILED``.

    ``result`` is inherited untyped and is null in every state. A finished training run's
    output is the trained artifacts, which are read per verse through
    ``GET /v4/training-sessions/{session_id}/results`` rather than returned inline; a
    ``SUCCEEDED`` job with no ``result`` is explicitly legal.
    """

    state: JobState = Field(
        description=(
            "The job's current public state; branch on this. Never null here: a job "
            "whose state cannot be read answers `TRAINING_JOB_STATE_UNAVAILABLE` instead "
            "of this body."
        ),
    )
    error: V4ErrorDetail | None = Field(
        default=None,
        description=(
            "Why the job failed — the same {code, message, details} object the v4 error "
            "envelope uses, with the generic `JOB_FAILED` code and the runner's own "
            "prose as its message. Non-null exactly when state is FAILED. Unlike the "
            "list row's field, this one never carries "
            "`TRAINING_JOB_STATE_UNAVAILABLE`: a job whose state cannot be read answers "
            "that as a 500 rather than as this body. Note that a FAILED poll is still "
            "HTTP 200 — reading the job succeeded, the job did not."
        ),
    )


class InferenceReadinessOut(V4BaseModel):
    """Whether one analysis can be run against this pair yet.

    "Ready" means a training job of that type for this revision pair has finished, so
    the artifacts inference needs exist. It is a property of the **pair**, not of the
    session: a type trained by an earlier session counts, and a type this session did not
    select can still be ready.
    """

    ready: bool = Field(
        description="Whether this analysis has finished training for this pair.",
    )
    pending_training: list[TrainingType] = Field(
        default_factory=list,
        description=(
            "Which training types must still finish before this analysis is ready. "
            "Empty when `ready`. Today every analysis depends on exactly its own type, "
            "so this is either empty or the single key it hangs under; it is a list "
            "because the dependency is the runner's to define and need not stay 1:1."
        ),
    )


class TrainingSessionOut(V4BaseModel):
    """The body of ``GET /v4/training-sessions/{session_id}``.

    A derived view over the jobs sharing a session key — see the module docstring for
    what that means for ``state`` and for why an empty session is a ``404``.
    """

    session_id: str = Field(
        description="The session key, echoed from the path.",
    )
    state: JobState | None = Field(
        default=None,
        description=(
            "The session's aggregate state, for a client that wants one thing to branch "
            "on: FAILED if any job failed, else RUNNING if any job is non-terminal, else "
            "SUCCEEDED. **Never PENDING** — a session that has been accepted but not "
            "started reports RUNNING, and `jobs[].state` is where the distinction lives. "
            "Null when any job's state could not be read, since an aggregate over an "
            "unknown outcome would be a guess; that job's own entry names the fault."
        ),
    )
    jobs: list[TrainingJobOut] = Field(
        description=(
            "Every job in the session, lowest id first. One per analysis the submit "
            "selected, minus any that were skipped as duplicates of an already-active "
            "job for the same pair and options."
        ),
    )
    inference_readiness: dict[TrainingType, InferenceReadinessOut] = Field(
        description=(
            "Which analyses can be run against this pair now, keyed by analysis. "
            "Computed over every finished training job for the pair, not only this "
            "session's — so a session can report an analysis ready that it did not run."
        ),
    )


class TrainingVerseScore(V4BaseModel):
    """A verse-level score on a results row.

    Carries neither the stored row's ``id`` nor its ``assessment_id``: nothing addresses
    a single result row, and the per-type assessment id is on the session's job list, so
    repeating it on every verse of every page is noise.
    """

    score: float | None = Field(
        default=None,
        description=(
            "The verse's score. Null if the row was stored without one — v3 reported "
            "such a row as `0.0`, which is a real score and a different claim."
        ),
    )
    flag: bool = Field(
        default=False,
        description="Whether the row was flagged for attention.",
    )
    hide: bool = Field(
        default=False,
        description=(
            "Whether the row is marked as hidden from display. Advisory — the row is "
            "still returned."
        ),
    )
    note: str | None = Field(
        default=None,
        description="Free-text note the runner attached to this verse, or null.",
    )


class TrainingWordAlignment(V4BaseModel):
    """One word pairing on a results row.

    A verse contributes as many of these as it has aligned source words, which is why
    they sit in a list beside the verse-level ``word_alignment_score`` rather than
    replacing it.
    """

    source: str | None = Field(
        default=None,
        description="The source-side word, as the runner stored it — lower-cased.",
    )
    target: str | None = Field(
        default=None,
        description="The target-side word this source word aligned to.",
    )
    score: float | None = Field(
        default=None,
        description=(
            "The alignment score for this pair, higher being a stronger alignment. Null "
            "if the row was stored without one."
        ),
    )
    flag: bool = Field(
        default=False,
        description="Whether the row was flagged for attention.",
    )
    hide: bool = Field(
        default=False,
        description="Whether the row is marked as hidden from display. Advisory.",
    )
    note: str | None = Field(
        default=None,
        description="Free-text note the runner attached to this pairing, or null.",
    )


class TrainingNeighbour(V4BaseModel):
    """One TF-IDF nearest neighbour of a results row's verse."""

    vref: str = Field(
        description=(
            "The neighbouring verse, as a canonical vref. Never the row's own verse, "
            "which is excluded from its own ranking."
        ),
    )
    similarity: float = Field(
        description=(
            "How close this verse is to the row's verse — the inner product of their "
            "PCA-reduced TF-IDF vectors, higher being more similar. **A ranking score, "
            "not a calibrated one**: comparable within one corpus, not across corpora. "
            "Do not threshold on it."
        ),
    )
    target_text: str | None = Field(
        default=None,
        description=(
            "The target revision's text for this verse, so a ranked list renders without "
            "a request per hit. Null where that revision has no text of its own for the "
            "verse."
        ),
    )
    source_text: str | None = Field(
        default=None,
        description="The same verse in the source revision, or null on the same terms.",
    )


class TrainingTfidfNeighbours(V4BaseModel):
    """The ``tfidf`` block of a results row: nearest neighbours from each side's corpus.

    Both sides rank the row's *own* verse against a corpus — the target side against this
    session's corpus, the source side against the corpus a separate session trained on
    this session's source revision. The source side is not a second pagination.
    """

    target_neighbours: list[TrainingNeighbour] = Field(
        default_factory=list,
        description=(
            "Neighbours within this session's trained corpus, most similar first. Empty "
            "where the verse has no neighbours — a one-verse corpus, say."
        ),
    )
    source_neighbours: list[TrainingNeighbour] | None = Field(
        default=None,
        description=(
            "Neighbours within the source-side corpus, or **null when no source-side "
            "TF-IDF training exists** for this pair. An empty list means the corpus "
            "exists and this verse has no neighbours in it — v3 collapsed those two into "
            "one empty list, so a client could not tell them apart."
        ),
    )


class TrainingNgramOccurrence(V4BaseModel):
    """One verse a trained n-gram occurs in, with both revisions' text for it."""

    vref: str = Field(
        description="The verse, as a canonical vref.",
    )
    target_text: str | None = Field(
        default=None,
        description=(
            "The target revision's text for this verse, or null where it has none of "
            "its own."
        ),
    )
    source_text: str | None = Field(
        default=None,
        description="The same verse in the source revision, or null on the same terms.",
    )


class TrainingNgramMatch(V4BaseModel):
    """One trained n-gram that fires on a results row's verse."""

    id: int = Field(
        description=(
            "The stored n-gram's id. Not a handle: no v4 endpoint addresses a single "
            "n-gram row on this surface."
        ),
    )
    ngram: str | None = Field(
        default=None,
        description="The n-gram itself, exactly as the runner stored it.",
    )
    ngram_size: int | None = Field(
        default=None,
        description="How many tokens the n-gram has — the *n*.",
    )
    occurrences: list[TrainingNgramOccurrence] = Field(
        default_factory=list,
        description=(
            "**Every** verse this n-gram occurs in across the corpus, not only the row's "
            "own verse — the same cross-corpus list `POST /v4/predictions` returns, so "
            "one n-gram's entry is identical on every row it fires on. Named to match "
            "`GET /v4/assessments/{id}/ngrams`' `occurrences`, and for the same reason: "
            "calling an occurrence list `vrefs` collides with the span coverage that "
            "field means on the result reads."
        ),
    )


class TrainingNgrams(V4BaseModel):
    """The ``ngrams`` block of a results row: trained n-grams firing on this verse.

    Stored corpus hits only. Predict's cross-axis matches — target n-grams found in
    source text and vice versa — are computed at inference time and are not returned
    here.
    """

    target_corpus: list[TrainingNgramMatch] = Field(
        default_factory=list,
        description=(
            "N-grams from this session's trained corpus that fire on this verse, most "
            "recently mined first is *not* guaranteed — the order is the corpus's."
        ),
    )
    source_corpus: list[TrainingNgramMatch] | None = Field(
        default=None,
        description=(
            "The same from the source-side corpus, or **null when no source-side n-gram "
            "training exists** for this pair. An empty list means the corpus exists and "
            "nothing fired on this verse."
        ),
    )


class TrainingResultRow(V4BaseModel):
    """One verse of ``GET /v4/training-sessions/{session_id}/results``.

    Every per-type field is null (or empty) unless that type's job in this session has
    **finished** — a queued, running or failed job contributes nothing here, and its
    state is visible on the session read instead. So a row full of nulls means "nothing
    has finished for this verse yet", not "no data exists".
    """

    vref: str = Field(
        description=(
            "The verse this row is about, as a canonical vref. Rows are in canonical "
            "Bible order — book, then chapter, then verse."
        ),
    )
    semantic_similarity: TrainingVerseScore | None = Field(
        default=None,
        description=(
            "The `semantic-similarity` job's score for this verse, or null when that "
            "type has not finished or scored no row here."
        ),
    )
    word_alignment: list[TrainingWordAlignment] = Field(
        default_factory=list,
        description=(
            "The `word-alignment` job's per-word pairings for this verse. Empty when "
            "that type has not finished, and also for a verse it aligned nothing in."
        ),
    )
    word_alignment_score: TrainingVerseScore | None = Field(
        default=None,
        description=(
            "The same job's verse-level score, which is stored separately from the "
            "per-word rows above. Null on the same terms."
        ),
    )
    tfidf: TrainingTfidfNeighbours | None = Field(
        default=None,
        description=(
            "The `tfidf` job's nearest neighbours for this verse, per side. Null when "
            "that type has not finished — distinct from a populated block whose lists "
            "are empty, which means it finished and found nothing."
        ),
    )
    ngrams: TrainingNgrams | None = Field(
        default=None,
        description=(
            "The `ngrams` job's trained n-grams firing on this verse, per corpus. Null "
            "when that type has not finished, on the same terms as `tfidf`."
        ),
    )
