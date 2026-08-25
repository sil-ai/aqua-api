"""v4 Assessment request schemas (issues #826/#830/#865/#893, epic #842).

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
"""

from typing import Annotated, Literal, Union

from pydantic import Field

from api_v4.schemas.base import V4BaseModel

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
        max_length=VREF_MAX_LENGTH,
        description="First verse of the range to critique, e.g. 'GEN 1:1'.",
    )
    last_vref: str | None = Field(
        default=None,
        max_length=VREF_MAX_LENGTH,
        description=(
            "Last verse of the range. Omit it to critique through to the end of the "
            "chapter — omitted and 'the whole chapter' are the same request, so this "
            "has no default value."
        ),
    )
    response_language: str | None = Field(
        default=None,
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


__all__ = [
    "RESPONSE_LANGUAGE_MAX_LENGTH",
    "VREF_MAX_LENGTH",
    "AgentCritiqueOptions",
    "AssessmentCreate",
    "AssessmentOptions",
    "AssessmentOptionsBase",
    "NgramsOptions",
    "ReferencedAssessmentOptions",
    "SemanticSimilarityOptions",
    "SentenceLengthOptions",
    "TextLengthsOptions",
    "TfidfOptions",
    "WordAlignmentOptions",
]
