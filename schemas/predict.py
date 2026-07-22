"""Predict fan-out request/response and job schemas (issue #729)."""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_serializer, model_validator


class TextPair(BaseModel):
    vref: Optional[str] = Field(default=None, max_length=50)
    source_text: Optional[str] = Field(default=None, max_length=10000)
    target_text: str = Field(..., max_length=10000)


class PredictInput(BaseModel):
    pairs: List[TextPair] = Field(..., min_length=1, max_length=5000)
    assessment_id: Optional[int] = None
    revision_id: Optional[int] = None
    reference_id: Optional[int] = None
    source_version_id: Optional[int] = None
    target_version_id: Optional[int] = None
    limit: Optional[int] = Field(default=None, ge=1, le=10000)
    apps: Optional[List[str]] = None
    include_translation: bool = True
    include_critique: bool = True

    model_config = {
        "json_schema_extra": {
            "example": {
                "pairs": [
                    {
                        "vref": "GEN 1:1",
                        "source_text": "In the beginning God created the heavens and the earth.",
                        "target_text": "Hapo mwanzo Mungu aliumba mbingu na dunia.",
                    }
                ],
                "revision_id": 1,
                "reference_id": 2,
                "source_version_id": 1,
                "target_version_id": 2,
                "apps": ["ngrams", "tfidf", "agent"],
                "include_translation": True,
                "include_critique": True,
            }
        }
    }

    @model_validator(mode="after")
    def _critique_requires_translation(self) -> "PredictInput":
        # Mirrors the agent-side validator in aqua-assessments
        # (shared/predict_input.py): critique runs over translations, so
        # asking for it without translation is a bug, not a silent no-op.
        # Reject early at the API boundary so the caller sees a 422 rather
        # than a per-app error string in the fan-out response.
        # Both flags default to True, so a caller opting out of translation
        # without mentioning critique means "fast path only" — turn critique
        # off with it rather than 422ing on the unset default. (The
        # assignment adds include_critique to model_fields_set, so after
        # validation that set no longer reflects only caller-sent fields.)
        if self.include_critique and not self.include_translation:
            if "include_critique" not in self.model_fields_set:
                self.include_critique = False
            else:
                raise ValueError(
                    "include_critique=True requires include_translation=True"
                )
        return self


class PredictAppResult(BaseModel):
    status: Literal["ok", "error", "not_trained"]
    data: Optional[Any] = None
    error: Optional[str] = None
    duration_ms: int


class PredictJobHandle(BaseModel):
    id: str
    status: Literal["running", "complete", "failed"]
    includes: List[Literal["translation", "critique"]]
    poll_url: str


class PredictFanoutResponse(BaseModel):
    pairs: List[TextPair]
    results: Dict[str, PredictAppResult]
    job: Optional[PredictJobHandle] = None

    @model_serializer(mode="wrap")
    def _drop_unset_job(self, handler):
        # The fast-only path (no include_translation/critique) shouldn't
        # surface a `"job": null` key in the response — keep the wire
        # shape identical to pre-async callers.
        data = handler(self)
        if data.get("job") is None:
            data.pop("job", None)
        return data


class PredictCritiqueIssue(BaseModel):
    """One MQM-aligned critique issue, as surfaced on a predict job's pair.

    Mirrors the per-issue shape the agent emits and the storage endpoint
    accepts (``POST /v3/agent/critique`` / ``IssueIn``). Extra fields the
    agent may add are preserved on the wire via ``extra="allow"`` rather
    than dropped, so consumers can rely on the documented fields while
    forward-compat new agent attributes still reach them.

    Field constraints deliberately diverge from ``IssueIn`` on the read
    path: ``dimension`` is typed as ``str`` (not a ``Literal``), and
    ``subtype`` / ``detector`` / ``severity`` carry no length or range
    bounds. Validation here runs over data the agent already wrote into
    a stored job result, so any extra-strict typing would convert a
    previously-200 response into a 500 if the agent ever emits an
    unexpected value (a new MQM dimension added agent-side before
    aqua-api, a legacy row predating a tightened constraint, etc.). The
    documented values live in each field's ``description`` — callers
    should match case-insensitively / by prefix.
    """

    dimension: str = Field(
        description=(
            "MQM dimension. Documented values: 'accuracy', 'terminology', "
            "'linguistic_conventions'. Typed as a plain string on the read "
            "path so an unexpected agent value can't 500 the poll endpoint; "
            "the documented enum is surfaced via json_schema_extra so "
            "Swagger UI still shows the canonical values."
        ),
        json_schema_extra={
            "enum": ["accuracy", "terminology", "linguistic_conventions"],
        },
    )
    subtype: str = Field(
        description=(
            "Free-form MQM subtype, e.g. 'omission', 'addition', "
            "'mistranslation', 'mistranslation/hallucination-numbers'. "
            "Not an enum — match case-insensitively / by prefix. The "
            "documented storage cap of 100 chars is advertised via "
            "json_schema_extra without being enforced on the read path."
        ),
        json_schema_extra={"maxLength": 100},
    )
    source_text: Optional[str] = None
    draft_text: Optional[str] = None
    comments: Optional[str] = None
    severity: Optional[int] = Field(
        default=None,
        description=(
            "Severity score the agent assigned, typically 1–5; null when "
            "the model omitted it. Range is advertised via "
            "json_schema_extra but not enforced on the read path (see "
            "class docstring)."
        ),
        json_schema_extra={"minimum": 1, "maximum": 5},
    )
    detector: Optional[str] = Field(
        default=None,
        description=(
            "Optional tag identifying the automated detector that "
            "flagged the issue, e.g. 'number_diff'. Documented length "
            "cap of 50 chars is advertised but not enforced."
        ),
        json_schema_extra={"maxLength": 50},
    )
    evidence: Optional[List[str]] = Field(
        default=None,
        description="Optional supporting snippets the detector or model attached.",
    )

    model_config = ConfigDict(extra="allow")


class PredictCritique(BaseModel):
    """The critique payload surfaced per pair on a predict job.

    Currently exposes ``issues`` (the canonical MQM list, see #793).
    ``extra="allow"`` keeps any auxiliary keys the agent emits visible to
    consumers; today there are none documented beyond ``issues``.
    """

    issues: List[PredictCritiqueIssue] = Field(default_factory=list)

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
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
    )


class PredictJobPair(BaseModel):
    vref: Optional[str] = None
    source_text: Optional[str] = None
    target_text: str
    translation: Optional[Dict[str, Any]] = None
    critique: Optional[PredictCritique] = None
    lexeme_cards: Optional[List[Dict[str, Any]]] = None


class PredictJobStatusResponse(BaseModel):
    id: str
    status: Literal["running", "complete", "failed"]
    includes: List[Literal["translation", "critique"]]
    pairs: List[PredictJobPair]
    error: Optional[str] = None


__all__ = [
    "TextPair",
    "PredictInput",
    "PredictAppResult",
    "PredictJobHandle",
    "PredictFanoutResponse",
    "PredictCritiqueIssue",
    "PredictCritique",
    "PredictJobPair",
    "PredictJobStatusResponse",
]
