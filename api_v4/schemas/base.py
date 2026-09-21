"""Shared base model for all v4 schemas (issue #830, epic #842).

v4 standardizes the wire contract on **snake_case** field names — the canonical
name of a field *is* its snake_case Python attribute, and that is what v4 emits.
This is the deliberate break from v3, whose schemas mixed conventions
(``machineTranslation``, ``forwardTranslation``, ...). v3 stays frozen; v4 fixes
the contract going forward.

:class:`V4BaseModel` supplies only the config every v4 schema needs:

* ``populate_by_name=True`` — a model can always be constructed (and validated)
  using its canonical snake_case field name, *regardless* of any alias a field
  later carries. This is what makes the per-domain migration safe: a field can
  gain an alias for its legacy v3 name without breaking internal callers that
  build the model by field name.

Scope of this PR is the base infrastructure only. It deliberately does **not**
migrate any real field names or attach deprecation aliases — that is per-domain
work in the contract issues (#825-#831).

Guidance for those later PRs (so snake_case stays canonical *on the wire*):
accept a legacy v3 name as an *input-only* ``validation_alias`` rather than a
plain ``alias``. FastAPI serializes responses with ``by_alias=True`` by default,
so a plain ``alias`` would push the legacy name back onto the wire — the exact
thing v4 is standardizing away from. ``validation_alias`` accepts the old name
on input while serialization keeps emitting the snake_case field name. Example::

    from pydantic import Field
    from api_v4.schemas.base import V4BaseModel

    class RevisionOut(V4BaseModel):
        machine_translation: bool = Field(
            default=False,
            validation_alias="machineTranslation",  # accept legacy v3 input
        )
    # -> emits {"machine_translation": ...}; accepts either name on input.

Second policy, added by #954: **no string anywhere in a v4 model may contain a
NUL byte** (``\\x00``). This one is not a naming convention but a bug fix.
Postgres ``text`` cannot hold ``\\x00`` in any column, so asyncpg raises
``CharacterNotInRepertoireError`` when a caller-supplied NUL reaches the driver —
at query time, inside the handler, past every validation layer — and the v4
catch-all (:mod:`api_v4.errors`) turns that into a 500 for what is plainly a bad
request. :meth:`V4BaseModel._reject_nul_bytes` moves the failure forward to
validation, where it is the 422 it always should have been.

It lives on the shared base rather than on the fields that need it because a
``model_validator`` is inherited *and still runs* on subclasses (the behaviour
``api_v4/schemas/training.py`` already documents relying on). One definition
therefore covers every v4 request body written so far and every one written
next, with nothing for a future author to remember. The rejected alternative — a
shared ``Annotated`` validator type applied field by field — silently no-ops
against this codebase's ``param: str = Query(...)`` style, so it would look
solved and quietly stop being solved on the next endpoint. Query strings and
path segments never pass through a model at all and are covered separately, by
the guard middleware in :mod:`api_v4.errors`.
"""

from collections import deque
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, model_validator
from pydantic_core import PydanticCustomError

#: The one character a Postgres ``text`` column cannot hold (#954). Every other C0
#: control character — ``\t``, ``\n``, ``\r``, and the rest — is legal in ``text``
#: and occurs in real Scripture payloads, so the check is scoped to this one byte and
#: deliberately not widened to "control characters".
NUL = "\x00"

#: The pydantic error ``type`` this raises, and the ``type`` the query/path guard in
#: :mod:`api_v4.errors` puts in its hand-built ``details.errors`` entry. Shared so the
#: two doors a NUL can arrive through report themselves identically: both are a ``422``
#: with ``code="VALIDATION_ERROR"``, and a client (or a log query) tells this class of
#: rejection from any other validation failure by this string, not by the code.
NUL_BYTE_ERROR_TYPE = "nul_byte_not_allowed"

#: The sentence both halves of the fix say. The guard middleware uses it verbatim —
#: its ``loc`` already names the offending parameter — and the model validator appends
#: the offending paths, which a body needs because ``loc`` can only reach the model.
NUL_BYTE_MESSAGE = (
    "Text must not contain a NUL byte (\\x00), which Postgres cannot store."
)

#: The validator's message template. ``{fields}`` is substituted from the error's
#: ``ctx`` by pydantic; braces *inside* the substituted value are not re-processed
#: (verified), so an offending dict key containing ``{`` cannot break the formatting.
NUL_BYTE_ERROR_TEMPLATE = NUL_BYTE_MESSAGE + " Offending values: {fields}."


class V4BaseModel(BaseModel):
    """Base class for every v4 request/response schema.

    See the module docstring for the contract rationale. Subclass this instead
    of ``pydantic.BaseModel`` directly so the whole v4 surface shares one
    canonical-name / alias policy.
    """

    model_config = ConfigDict(
        # Build/validate by the canonical snake_case field name even when a
        # field also defines an alias (e.g. a legacy-v3-name deprecation alias
        # added by a later per-domain PR). Without this, once a field has an
        # alias, constructing the model by its Python field name would raise.
        populate_by_name=True,
    )

    #: Whether :meth:`_reject_nul_bytes` runs on this model (#954). True for every
    #: v4 schema but one; see :class:`api_v4.errors.V4ErrorDetail`, which turns it off
    #: because the error envelope is the one model that must never refuse to serialize.
    #: A ``ClassVar`` rather than a ``model_config`` key so it is inherited normally and
    #: so pydantic treats it as a class attribute instead of a field.
    _checks_nul_bytes: ClassVar[bool] = True

    @model_validator(mode="after")
    def _reject_nul_bytes(self) -> "V4BaseModel":
        """Reject any string in this model that contains a NUL byte (#954).

        See the module docstring for why the rule exists and why it lives here. This
        method is the *body* half of the fix; query strings and path segments are the
        other half and are handled by :func:`api_v4.errors.register_nul_byte_guard`,
        because neither is ever parsed through a model.

        Raising here produces a ``RequestValidationError``, which
        :func:`api_v4.errors._handle_validation_error` already shapes into the 422
        envelope with the failure under ``details.errors`` — so this deliberately does
        not build a response. ``loc`` on that entry is the path to *this model*
        (``["body"]``, or e.g. ``["body", "text"]`` for a nested one) and ``ctx.fields``
        names the offending values within it, which together give the full path.

        Cost, measured on both sides rather than argued, because the two sides are
        limited by different things:

        * **Requests** are limited by string *size*, and size turns out not to matter.
          Scanning the ~6.3M-character ``content_base64`` of a full-Bible
          ``POST /v4/revisions`` takes 0.25 ms worst case — ``str.__contains__`` on a
          single character is a memchr — against 0.4 ms to validate that body at all,
          and seconds for the decode and 41,899 inserts that follow.
        * **Responses** are limited by the *number of models*, and that is where the
          real cost is: about 5 µs per model, which on a full 1000-row page of
          ``VerseOut`` is ~8 ms and roughly doubles that page's validation time. It is
          bounded — every model-returning v4 read is paginated at 1000 rows, and the one
          unpaginated read returns ``text/plain``, not models — and it is accepted
          rather than optimized away. A scalar fast path was tried and recovered only
          17% of it; the rest is pydantic's per-model validator dispatch, which no
          cleverness inside the validator can avoid. Exempting response models via
          :attr:`_checks_nul_bytes` would avoid it, and is deliberately not done: the
          exemption list is asserted to have exactly one entry so that turning the check
          off somewhere is a decision, not a habit.
        """
        if not type(self)._checks_nul_bytes:
            return self
        roots = [(name, value) for name, value in self.__dict__.items()]
        # ``extra="allow"`` models keep unmodelled keys in __pydantic_extra__ rather than
        # in __dict__ (verified), and an extra key is caller-supplied text exactly as a
        # nested dict key is — so it is checked on the same terms rather than merely
        # escaped for display. No request body allows extras today (every one declares
        # extra="forbid"), so this covers only response models now; it is here so the
        # rule grows no hole the day one opens up, which is the same reason the check
        # is on the shared base at all. Field names need no such check: they are Python
        # identifiers.
        found = []
        for name, value in (self.__pydantic_extra__ or {}).items():
            label = escape_nul_bytes(name)
            if NUL in name:
                found.append(f"{label} (key)")
            roots.append((label, value))
        found += _nul_byte_paths(roots)
        if found:
            raise PydanticCustomError(
                NUL_BYTE_ERROR_TYPE,
                NUL_BYTE_ERROR_TEMPLATE,
                {"fields": found},
            )
        return self


def escape_nul_bytes(key: str) -> str:
    """``key`` with any NUL rendered as the literal text ``\\x00``.

    Public because the guard middleware in :mod:`api_v4.errors` needs it for the same
    reason this module does: both echo a caller-supplied name back — a dict key here, a
    query parameter name there — and answering "your string contains a NUL" with a body
    that itself contains one is not an answer. Field names never need it; they are
    Python identifiers.
    """
    return key.replace(NUL, "\\x00")


def _nul_byte_paths(roots: list[tuple[str, Any]]) -> list[str]:
    """The dotted path of every string under ``roots`` that holds a NUL.

    ``roots`` is ``(path, value)`` pairs — the top level's fields. Returns the paths in
    breadth-first order, which is stable across runs and reads as "shallowest first".

    **Iterative, not recursive, and that is load-bearing.**
    ``TrainingSessionCreate.options`` is ``dict[str, Any]`` (``api_v4/schemas/training.py``):
    arbitrary caller-controlled keys and values, nested as deeply as the client likes,
    and ``api_v4/errors.py`` records that a client can get a body roughly 900 levels deep
    past the JSON parser. A recursive walk would exhaust the stack on such a body, and a
    ``RecursionError`` is not a ``ValueError``, so pydantic would not convert it — it
    would reach the catch-all as a 500 raised *by the check that exists to prevent 500s*.
    An explicit queue has no depth limit, so the check can never itself be the reason a
    request fails.

    **Stops at any nested** :class:`V4BaseModel`: a nested model runs its own inherited
    copy of the validator during the parent's validation, so descending into it would
    re-walk a subtree that has already cleared itself — for ``RevisionCreate`` that would
    mean scanning the megabyte payload twice. A nested plain ``BaseModel`` *is* walked,
    since it has no validator of its own to inherit. Nothing on the v4 request surface
    nests one today (audited); the branch is here so the rule does not quietly stop
    holding if one appears.

    That skip rests on an assumption worth naming, because pydantic offers two ways to
    break it: ``model_construct()`` and ``model_copy()`` both build an instance without
    running validators, and pydantic v2 defaults ``revalidate_instances`` to ``"never"``,
    so nesting such an instance inside a parent does not re-validate it either. A NUL
    smuggled in that way would be invisible at every level. Neither call appears anywhere
    in this repository (checked), which is why the skip is safe today; if one is
    introduced on a path that carries caller data, this is the line it invalidates.

    Dict **keys** are checked as well as values, because ``extra="forbid"`` reports an
    unknown key by putting it in the error ``loc`` — so a key is caller-supplied text on
    exactly the same terms as a value.

    ``bytes`` is not walked: no v4 request field is annotated ``bytes``, and the one
    place bytes carry caller text — ``InlineText.content_base64`` after decoding — is
    checked at the decode itself, in
    ``bible_routes.v4.revision_service.decode_verse_text``, because a base64 *string*
    never contains the NUL its decoded bytes do.
    """
    found: list[str] = []
    pending = deque(roots)
    while pending:
        path, value = pending.popleft()
        if isinstance(value, str):
            if NUL in value:
                found.append(path)
        elif isinstance(value, BaseModel):
            if not isinstance(value, V4BaseModel):
                pending.extend(
                    (f"{path}.{name}", attr) for name, attr in value.__dict__.items()
                )
        elif isinstance(value, dict):
            for key, item in value.items():
                if isinstance(key, str):
                    label = escape_nul_bytes(key)
                    if NUL in key:
                        found.append(f"{path}.{label} (key)")
                else:
                    label = repr(key)
                pending.append((f"{path}.{label}", item))
        elif isinstance(value, (list, tuple, set, frozenset)):
            pending.extend(
                (f"{path}[{index}]", item) for index, item in enumerate(value)
            )
    return found
