"""v4 Bible-domain request/response schemas (issues #825/#826/#830/#891/#892, epic #842).

The Bible-domain slices on the v4 surface are Versions, Revisions, and Verses &
text. These schemas subclass :class:`api_v4.schemas.base.V4BaseModel`, so the wire contract is
**snake_case** (issue #830): every field's canonical name is its snake_case Python
attribute, and that is what v4 emits.

Legacy v3 spellings that were camelCase (``forwardTranslation``,
``backTranslation``, ``machineTranslation``) are accepted on *input* via a
``validation_alias`` of ``AliasChoices(<snake_case>, <legacy camelCase>)`` so
existing callers can migrate without a flag day. Two properties matter, and both
are load-bearing for the #830 goal:

* ``validation_alias`` (not a plain ``alias``) is input-only, so responses keep
  emitting snake_case — see the :class:`V4BaseModel` docstring for why a plain
  ``alias`` would leak the legacy name back onto the wire.
* Listing the **snake_case name first** in ``AliasChoices`` makes it the property
  name in the generated OpenAPI request schema (FastAPI serializes schemas with
  ``by_alias=True``, and Pydantic uses the first choice), so ``/v4/openapi.json``
  documents the canonical snake_case field while still accepting the legacy name.
  A bare ``validation_alias="machineTranslation"`` would validate fine but
  document only the *deprecated* spelling — the opposite of #830.
"""

# Aliased because ``RevisionOut`` has a field *named* ``date``. In a class body the
# value assignment is stored before the annotation is evaluated, so a bare
# ``date: date | None = None`` resolves ``date`` to the just-stored ``None`` and raises
# ``TypeError: unsupported operand type(s) for |`` at import time.
from datetime import date as date_type
from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import AliasChoices, Field, field_validator, model_validator

from api_v4.schemas.base import V4BaseModel

#: Shared by ``VersionCreate`` and ``VersionPatch``. ``back_translation_id`` is a
#: FK to ``bible_version.id`` but ``forward_translation_id`` is a plain Integer
#: column (``database/models.py``), so the two adjacent, identically-typed request
#: fields validate differently: a non-existent ``back_translation`` is rejected
#: with ``INVALID_REFERENCE`` on flush, while a non-existent
#: ``forward_translation`` is stored and echoed back. That is why
#: ``InvalidReference.FIELDS`` omits it. Surfaced as a field description so a
#: client can tell which of the two is checked without reading the schema.
FORWARD_TRANSLATION_DESCRIPTION = (
    "Id of the forward-translation version. Unlike back_translation, this field is "
    "not backed by a foreign key, so a non-existent id is accepted and stored as "
    "given rather than rejected."
)


class VersionCreate(V4BaseModel):
    """Request body for ``POST /v4/versions`` (issue #826: JSON-only bodies).

    Snake_case is canonical; the three formerly-camelCase fields also accept
    their legacy v3 spelling on input via ``validation_alias``.
    """

    name: str
    # max_length mirrors the varchar(3) / varchar(4) columns, and it is load-bearing
    # rather than cosmetic: an over-length code reaches Postgres, which raises
    # StringDataRightTruncation -> SQLAlchemy DataError. DataError is a *sibling* of
    # IntegrityError, not a subclass, so the FK translation in create_version /
    # update_version cannot catch it and client input would surface as a catch-all
    # 500 (#828). Bounding the length here makes it a 422 before the write. The FK
    # check against the iso reference tables still happens on flush; this only
    # bounds the length.
    iso_language: str = Field(max_length=3)
    iso_script: str = Field(max_length=4)
    abbreviation: str
    rights: str | None = None
    # Legacy v3 camelCase names accepted on input; canonical (and emitted) name
    # stays snake_case. snake_case is listed first in AliasChoices so it is the
    # name documented in the OpenAPI schema (see the module docstring).
    forward_translation: int | None = Field(
        default=None,
        validation_alias=AliasChoices("forward_translation", "forwardTranslation"),
        description=FORWARD_TRANSLATION_DESCRIPTION,
    )
    back_translation: int | None = Field(
        default=None,
        validation_alias=AliasChoices("back_translation", "backTranslation"),
    )
    machine_translation: bool = Field(
        default=False,
        validation_alias=AliasChoices("machine_translation", "machineTranslation"),
    )
    is_reference: bool = False
    transcribed_audio: bool = False
    # Required, mirroring v3: a version must be created into at least one group
    # the caller belongs to. An empty list is a domain error (400), a missing
    # field is a framework validation error (422) — see version_service /
    # version_routes.
    add_to_groups: list[int]

    model_config = {
        **V4BaseModel.model_config,
        "json_schema_extra": {
            "example": {
                "name": "English King James Version",
                "iso_language": "eng",
                "iso_script": "Latn",
                "abbreviation": "english_-_king_james_version",
                "machine_translation": False,
                "add_to_groups": [1],
            }
        },
    }


class VersionPatch(V4BaseModel):
    """Request body for ``PATCH /v4/versions/{id}`` (issue #897).

    The field half of v3's overloaded ``PUT /version``. Every field is optional:
    only the ones actually present in the request body are written (the service
    uses ``model_dump(exclude_unset=True)``), so a one-field rename sends one
    field and everything else keeps its stored value.

    **Closed allowlist** (``extra="forbid"``, the one place v4 diverges from the
    permissive default the other schemas use). This fixes two v3 defects at once:

    * v3's ``VersionUpdate`` documented ``is_reference`` as patchable but had no
      such field, so requests setting it were silently ignored. Here the set of
      patchable fields *is* the wire contract, and an unknown name is a 422 rather
      than a silent no-op — a misspelled ``naem`` can never look like success.
    * v3 fed whatever survived ``exclude_unset`` straight into
      ``.values(**version_data)``, ``id`` included. The identity and lifecycle
      fields are simply absent here, so ``id`` / ``owner_id`` / ``deleted`` are
      rejected with 422 instead of written: ``id`` is the URL, ownership is not
      transferable through a field patch, and soft-delete has its own endpoint
      (``DELETE /v4/versions/{id}``). Group access likewise moved out, to the
      ``/v4/versions/{id}/groups/{group_id}`` sub-resource — so no
      ``add_to_groups`` / ``remove_from_groups`` here either.

    **``str``/``bool`` annotated with a ``None`` default is deliberate** for the
    fields whose column must not become NULL (a NULL ``name`` would fail
    ``VersionOut``'s required ``str``). Pydantic does not validate defaults, so the
    field is *absent-able* while an explicit ``null`` in the body fails validation
    (422) instead of nulling the column. The genuinely nullable fields
    (``rights``, ``forward_translation``, ``back_translation``) are typed
    ``| None`` and *can* be cleared by sending an explicit ``null``.

    One cosmetic consequence, called out so nobody "fixes" it: Pydantic emits a
    ``default`` for those fields, so the generated schema reads e.g.
    ``{"type": "string", "default": null}``. It does *not* reach
    ``/v4/openapi.json`` — FastAPI serializes the whole document with
    ``exclude_none=True`` (``fastapi/openapi/utils.py``), which strips every null
    value, this ``default`` included — but anything else that renders the schema
    (``model_json_schema()``, a contract-export script, a future FastAPI that stops
    excluding nulls) will show it. Harmless either way: ``default`` is a JSON-Schema
    *annotation*, never validated against ``type``, so the document stays valid and
    the ``type`` still tells a client that ``null`` is not an accepted value — which
    is exactly the behavior.
    """

    # See the class docstring: annotation excludes None (explicit null -> 422),
    # default None means "field absent" and is dropped by exclude_unset.
    name: str = None
    # max_length as on VersionCreate, for the same reason (an over-length code is a
    # DataError the IntegrityError handler cannot see -> 500). Pydantic does not
    # validate defaults, so pairing it with the None default is safe: the field
    # stays absent-able, an explicit null is still a 422, and only a real
    # over-length string is rejected.
    iso_language: str = Field(default=None, max_length=3)
    iso_script: str = Field(default=None, max_length=4)
    abbreviation: str = None
    # Nullable on purpose: sending an explicit null clears these.
    rights: str | None = None
    # Legacy v3 camelCase names accepted on input, as on VersionCreate; the
    # canonical (and emitted) spelling stays snake_case (#830).
    forward_translation: int | None = Field(
        default=None,
        validation_alias=AliasChoices("forward_translation", "forwardTranslation"),
        description=FORWARD_TRANSLATION_DESCRIPTION,
    )
    back_translation: int | None = Field(
        default=None,
        validation_alias=AliasChoices("back_translation", "backTranslation"),
    )
    machine_translation: bool = Field(
        default=None,
        validation_alias=AliasChoices("machine_translation", "machineTranslation"),
    )
    is_reference: bool = None
    transcribed_audio: bool = None

    model_config = {
        **V4BaseModel.model_config,
        # The allowlist is closed: unknown or non-patchable fields (id, owner_id,
        # deleted, add_to_groups, ...) are a 422, never silently dropped. See the
        # class docstring.
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "name": "English King James Version (revised)",
                "rights": "Public Domain",
            }
        },
    }


class VersionOut(V4BaseModel):
    """Response body for the ``/v4/versions`` endpoints.

    Plain snake_case fields — no aliases. The router builds this from the ORM
    row explicitly (see ``version_routes._to_out``) rather than validating the
    ORM object, so the ORM-attribute-name differences (``forward_translation_id``
    -> ``forward_translation``) are handled in one obvious place and this stays a
    pure output contract.
    """

    id: int
    name: str
    iso_language: str
    iso_script: str
    abbreviation: str
    rights: str | None = None
    forward_translation: int | None = None
    back_translation: int | None = None
    machine_translation: bool = False
    is_reference: bool = False
    transcribed_audio: bool = False
    owner_id: int | None = None
    group_ids: list[int] = Field(default_factory=list)
    deleted: bool = False
    # Delta-sync watermark (#887/#897): maintained by the BEFORE UPDATE trigger on
    # bible_version, so it moves on every write path including soft-delete. Optional
    # on the wire because the column is only NOT NULL going forward — a legacy row
    # from before the column existed may still hold NULL, the same reason _to_out
    # coerces the nullable booleans with bool(...). Clients feed the maximum value
    # they have seen back as ``updated_since``; see ``GET /v4/versions``.
    updated_at: datetime | None = None


# --- Revisions (issue #891) --------------------------------------------------

#: Maximum size of an upload's *decoded* verse text. Mirrors v3's
#: ``MAX_UPLOAD_BYTES`` (50MB) so the JSON upload is not a way to push a payload
#: v3's multipart route would have refused. A full-Bible plaintext is ~5MB, so this
#: is ~10x headroom.
MAX_TEXT_BYTES = 50 * 1024 * 1024
#: The cap actually enforced, expressed in base64 characters: base64 encodes 3 bytes
#: as 4 characters, so a string this long cannot decode to more than
#: :data:`MAX_TEXT_BYTES`. Enforcing it on the *encoded* string means one bound, on
#: the value Pydantic already has, instead of a second check after decoding.
#:
#: Note what this does and does not buy. It bounds the decode, the parse, and the
#: verse INSERTs. It does **not** bound the read: FastAPI reads and parses the whole
#: JSON body *before* dependencies or handler code run, so v3's streaming
#: pre-buffer rejection (#767, 413 before the body is consumed) has no route-layer
#: equivalent on a JSON endpoint. A true pre-buffer cap belongs in front of the app
#: (proxy / App Runner request-body limit) and is deliberately not reimplemented here.
MAX_CONTENT_BASE64_CHARS = -(-MAX_TEXT_BYTES // 3) * 4


class InlineText(V4BaseModel):
    """Verse text carried inline in the request body, base64-encoded.

    The ``text`` of a ``POST /v4/revisions`` (issue #826: JSON-only bodies — no
    multipart). ``type`` is a discriminator that exists from day one so the *other*
    source named in migration-guide §6 — an S3 reference, AERO's ``AudioSource``
    pattern — can be added later as another member of a discriminated union. That is
    an additive change to the request schema; a client sending ``type: "inline"``
    today keeps working unchanged. Adding the alternative *after* shipping a bare
    object with no discriminator would not have been additive, which is the whole
    reason the field is here before there is anything to discriminate against.

    Only ``inline`` exists today: nothing in this repository can fetch from S3 (no
    client, no bucket configuration), so declaring an ``s3`` variant would document
    a source the server would then fail to honor.
    """

    type: Literal["inline"] = "inline"
    content_base64: str = Field(
        max_length=MAX_CONTENT_BASE64_CHARS,
        description=(
            "Base64-encoded, vref-aligned UTF-8 plaintext: one line per verse "
            "reference in fixtures/vref.txt (41,899 lines), blank for a verse with "
            "no text. Line-wrapped base64 is accepted (whitespace is ignored on "
            "decode, but counts toward the length cap). The decoded text is capped "
            f"at {MAX_TEXT_BYTES} bytes."
        ),
    )


class RevisionCreate(V4BaseModel):
    """Request body for ``POST /v4/revisions`` (issues #826/#891).

    One JSON body, replacing v3's multipart ``file=`` upload plus its
    ``RevisionIn = Depends()`` form/query fields. ``date`` is not a request field: it
    is stamped server-side, exactly as v3 does.
    """

    # Canonical name is version_id — it matches the ``version_id`` list filter and
    # v3's own *request* field. v3's *response* spelled the same thing
    # ``bible_version_id`` (the ORM column name); that spelling is accepted here as
    # an input alias so a client migrating off v3's response shape can echo it back.
    version_id: int = Field(
        validation_alias=AliasChoices("version_id", "bible_version_id"),
    )
    name: str | None = None
    published: bool = False
    # FK to bible_revision.id (not bible_version.id — the back translation of a
    # revision is another revision), so a non-existent id is INVALID_REFERENCE on
    # flush. Legacy camelCase accepted on input, snake_case emitted (#830).
    back_translation: int | None = Field(
        default=None,
        validation_alias=AliasChoices("back_translation", "backTranslation"),
    )
    machine_translation: bool = Field(
        default=False,
        validation_alias=AliasChoices("machine_translation", "machineTranslation"),
    )
    # Required: a revision exists to hold verse text, and v3 equally required the
    # file. There is no create-empty-then-upload flow to preserve.
    text: InlineText

    model_config = {
        **V4BaseModel.model_config,
        "json_schema_extra": {
            "example": {
                "version_id": 1,
                "name": "June 2024",
                "published": False,
                "machine_translation": False,
                "text": {
                    "type": "inline",
                    # base64("In the beginning...\n\n") — a 2-line stand-in; a real
                    # body carries all 41,899 vref lines.
                    "content_base64": "SW4gdGhlIGJlZ2lubmluZy4uLgoK",
                },
            }
        },
    }


class RevisionPatch(V4BaseModel):
    """Request body for ``PATCH /v4/revisions/{id}`` (issue #891).

    Replaces v3's ``PUT /revision?id=&new_name=`` — a rename driven entirely by
    query parameters. The body-shaped version generalizes for free: the same closed
    allowlist that makes ``{"name": ...}`` work also covers the other three mutable
    fields, so toggling ``published`` no longer needs its own endpoint.

    **Closed allowlist** (``extra="forbid"``), for the reasons spelled out on
    :class:`VersionPatch`: an unknown or non-patchable field is a 422, never a silent
    no-op. Two fields are deliberately absent rather than merely unmentioned:

    * ``version_id`` — reparenting a revision would move it into a different
      authorization scope (read access is granted per *version*), so it is not a
      field edit. It has no endpoint at all today.
    * ``deleted`` — soft-delete has its own endpoint (``DELETE /v4/revisions/{id}``),
      and there is no un-delete.

    ``date`` is likewise absent: it records when the revision was uploaded.

    The ``bool`` fields are annotated without ``None`` while defaulting to ``None``
    — the :class:`VersionPatch` idiom: absent-able (``exclude_unset`` drops them),
    but an explicit ``null`` in the body is a 422 rather than a NULLed column.
    ``name`` and ``back_translation`` *are* nullable on the wire, matching
    ``RevisionCreate``, so an explicit ``null`` clears them.
    """

    name: str | None = None
    published: bool = None
    back_translation: int | None = Field(
        default=None,
        validation_alias=AliasChoices("back_translation", "backTranslation"),
    )
    machine_translation: bool = Field(
        default=None,
        validation_alias=AliasChoices("machine_translation", "machineTranslation"),
    )

    model_config = {
        **V4BaseModel.model_config,
        "extra": "forbid",
        "json_schema_extra": {"example": {"name": "June 2024 (revised)"}},
    }


class RevisionOut(V4BaseModel):
    """Response body for the ``/v4/revisions`` endpoints.

    Plain snake_case fields, no aliases — and, per #891, built **explicitly** from
    named ORM columns by ``revision_routes._to_out`` rather than by splatting
    ``revision.__dict__`` the way v3's ``create_revision_out`` does. v3's splat does
    not leak today only because this field set is closed and Pydantic ignores extras;
    it would start leaking the instant anyone set ``extra="allow"`` (the fragility
    behind #859). Naming the columns removes the question.

    ``version_abbreviation`` and ``iso_language`` are denormalized from the parent
    version, as in v3, so listing revisions does not require a second round of
    ``GET /v4/versions`` calls to label them.

    One field v3 emits is deliberately gone:

    * ``is_reference`` — a v3 *phantom*. ``RevisionOut_v3`` declares it, but
      ``bible_revision`` has no such column (``is_reference`` belongs to
      ``bible_version``), so v3's splat never populates it and every v3 revision
      reports ``is_reference: false`` regardless of its version. Emitting a constant
      false is worse than not emitting it; a client that wants the flag reads it from
      the version, where it actually lives.
    Only one v3 field is deliberately gone; ``updated_at`` arrived with the #899
    watermark contract and is now emitted here, same as on ``VersionOut``.
    """

    id: int
    version_id: int
    name: str | None = None
    # The column is DateTime; ``_to_out`` narrows it to a date rather than letting
    # Pydantic coerce, because coercion *raises* on a datetime with a non-zero time
    # component and legacy rows are not guaranteed to be midnight. v3 emits a date
    # here too, so the wire shape is unchanged.
    date: date_type | None = None
    published: bool = False
    back_translation: int | None = None
    machine_translation: bool = False
    deleted: bool = False
    version_abbreviation: str | None = None
    iso_language: str | None = None
    # Nullable, and NOT coerced: NULL is meaningful (a legacy row predating the
    # column), and a mirror must be able to tell "never stamped" from a real
    # timestamp — max() skips NULLs, so such a row simply cannot advance a
    # watermark. Mirrors VersionOut.updated_at.
    updated_at: datetime | None = None


# --- Verses & text (issue #892) ----------------------------------------------

#: Length of a USFM book abbreviation. Every entry in ``fixtures/book_reference.txt``
#: is exactly three characters (``GEN``, ``1SA``, ``3JN``), which is what makes an
#: *exact* length the right validation: v3 checks only ``len(book) > 3``, so it accepts
#: ``"G"`` and turns it into an empty result set.
#:
#: Lives here, in the Bible domain, and is imported by
#: :mod:`api_v4.schemas.assessment`: a book abbreviation is a Bible-domain fact
#: whichever read is scoping by it, and both the results read (#893) and the verses
#: read (#892) need the same bound.
BOOK_ABBREVIATION_LENGTH = 3

#: Maximum number of references a single ``vrefs`` filter may carry (#867, settled on
#: #892). A vref costs ~18-20 bytes once percent-encoded with its ``&vrefs=`` overhead,
#: so 1000 is ~20 KB of URL — comfortably under the ~60 KB platform ingress ceiling,
#: with 500-per-request already proven in production by the one known client. It is
#: also :data:`api_v4.pagination.VERSE_MAX_LIMIT`, so a caller who asks for the maximum
#: number of references can receive them all in a single page.
#:
#: **The limit is not the point; the error is.** v3 accepts an unlimited list, but past
#: roughly 3,000 references the URL exceeds the platform ingress limit and is rejected
#: *before reaching the application* — the caller gets a non-JSON body and the server
#: logs nothing. A stated limit that answers with a 422 naming both numbers converts a
#: silent infrastructure rejection into an answerable API response. A ``POST`` variant
#: was considered and rejected: a POST that creates nothing cuts against v4's
#: "verbs live in the HTTP method" rule and gives two ways to perform one read.
MAX_VREFS = 1000


class IncludeVerses(str, Enum):
    """Which verses ``GET /v4/revisions/{id}/verses`` returns.

    v3's flag has a third value, ``intersection``, which v3's own docs describe as
    "treated identically to 'union' for a single revision". It is a *cross-revision*
    set operation, and the only endpoint that could give it meaning — v3's
    ``GET /texts`` — is cut from v4 (no client has ever called it). A value that can
    never differ from another value is not a choice, so v4 has two.
    """

    #: Every canonical verse reference, whether or not this revision has text for it.
    all = "all"
    #: Only the verses this revision actually has text for. The default.
    union = "union"


class VerseScope(V4BaseModel):
    """Which verses to return from ``GET /v4/revisions/{id}/verses``.

    The five parameters cover what v3 spread across five endpoints — ``/verse``,
    ``/chapter``, ``/book``, ``/text`` and ``/vrefs`` — and the invariants that make
    them coherent hold **by construction** here rather than as runtime checks, the same
    way :class:`api_v4.schemas.assessment.ResultScope` does it for the results read:

    1. ``chapter`` requires ``book``
    2. ``verse`` requires ``book`` (and ``chapter``)
    3. ``vrefs`` cannot be combined with ``book`` / ``chapter`` / ``verse``

    Rule 3 is the one v3 has no opinion on, because in v3 the two were different
    endpoints. They are two ways of naming a set of verses, and combining them can only
    intersect one with the other — which returns fewer rows than either filter alone
    suggests, silently. Rejecting is also the reversible direction: allowing the
    combination later is not a breaking change, forbidding it later would be. The stated
    use case for ``vrefs`` is a whole-Bible word search, whose references land wherever
    the word occurs, so nothing about it wants a book filter as well.

    ``include_verses`` composes with all of them, which *is* new: v3 offered the flag
    only on ``/text``, its whole-revision read. ``include_verses=all`` with ``book=MAT``
    is every canonical verse of Matthew including the ones this revision lacks, and with
    ``vrefs=[...]`` it is a direct answer to "which of these references does this
    revision have?".

    A well-formed ``book`` naming no book, or a ``vrefs`` entry naming no canonical
    verse, yields an empty page rather than a 404: these narrow an already-authorized
    set instead of naming the collection's parent. Only the revision id in the path can
    404.
    """

    book: str | None = Field(
        default=None,
        min_length=BOOK_ABBREVIATION_LENGTH,
        max_length=BOOK_ABBREVIATION_LENGTH,
        description=(
            "Restrict to one book, as its three-letter USFM abbreviation (`MAT`). "
            "Case-insensitive. A well-formed abbreviation that names no book yields an "
            "empty page, not a 404. Cannot be combined with `vrefs`."
        ),
    )
    chapter: int | None = Field(
        default=None,
        ge=1,
        description="Restrict to one chapter. Requires `book`; excludes `vrefs`.",
    )
    verse: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Restrict to one verse. Requires `book` and `chapter`; excludes `vrefs`."
        ),
    )
    vrefs: list[str] | None = Field(
        default=None,
        description=(
            f"Restrict to this explicit list of verse references (`GEN 1:1`), for a "
            f"scattered selection no book/chapter filter can express. Case-insensitive. "
            f"**At most {MAX_VREFS} per request**; more is a 422 naming the limit and "
            f"the number received. Cannot be combined with `book`, `chapter` or "
            f"`verse`. A reference naming no canonical verse simply matches nothing. "
            f"Under the default `include_verses=union` a requested reference the "
            f"revision has no text for is absent from the page; use "
            f"`include_verses=all` to have every requested reference come back, with "
            f"empty text where there is none."
        ),
    )
    include_verses: IncludeVerses = Field(
        default=IncludeVerses.union,
        description=(
            "`union` (the default) returns only the verses this revision has text for, "
            "with verses the publisher merged into a neighbour folded into that "
            "neighbour's row. `all` returns every canonical verse in scope — all 41,899 "
            "of them when nothing else narrows the request — with empty text where the "
            "revision has none, and performs no merging: every row covers exactly one "
            "verse. Join against a result set using the default."
        ),
    )

    @field_validator("book")
    @classmethod
    def _upper_book(cls, value: str | None) -> str | None:
        """Normalize the abbreviation to upper case.

        Stored book values come from ``fixtures/vref.txt`` via ``bible_loading``, so
        they are always upper case. Normalizing the *input* once here lets the query
        compare the reference column directly rather than wrapping it in ``upper()``,
        which would forfeit the index.
        """
        return value.upper() if value is not None else None

    @field_validator("vrefs")
    @classmethod
    def _bounded_vrefs(cls, value: list[str] | None) -> list[str] | None:
        """Enforce :data:`MAX_VREFS` and normalize each reference.

        The bound is **not** declared as a ``max_length`` on the field, and that is
        deliberate: a field constraint runs before this validator and would win, and its
        message ("List should have at most 1000 items") names the limit but not what the
        caller actually sent. Naming both numbers is the entire value of the cap (see
        :data:`MAX_VREFS`), so the check lives where it can say them. The limit is
        advertised to clients through the field description and the OpenAPI parameter,
        not through a constraint keyword.

        Upper-casing matches the ``book`` filter's case-insensitivity: a canonical vref
        is an upper-case book abbreviation followed by digits, a space and a colon, so
        ``.upper()`` is total over well-formed input and idempotent. Malformed entries
        are not rejected — they simply match nothing, the same answer as a well-formed
        reference to a verse the revision lacks.
        """
        if value is None:
            return None
        if len(value) > MAX_VREFS:
            raise ValueError(
                f"at most {MAX_VREFS} vrefs per request; received {len(value):,}"
            )
        return [vref.strip().upper() for vref in value]

    @model_validator(mode="after")
    def _consistent_scope(self) -> "VerseScope":
        if self.chapter is not None and self.book is None:
            raise ValueError("chapter requires book")
        if self.verse is not None and self.chapter is None:
            raise ValueError("verse requires book and chapter")
        if self.vrefs is not None and (
            self.book is not None or self.chapter is not None or self.verse is not None
        ):
            raise ValueError("vrefs cannot be combined with book, chapter or verse")
        return self


class VerseOut(V4BaseModel):
    """One row of ``GET /v4/revisions/{id}/verses``.

    ``vref`` and ``vrefs`` are deliberately the **same pair, with the same meaning**, as
    on :class:`api_v4.schemas.assessment.AssessmentResultOut`. That is the published
    guarantee behind this read: a client can inner-join a result set to a verse set on
    ``vref`` and get the text that was actually scored, because both surfaces agree on
    what a merged span is called and which verses it covers.

    * **``vref`` is the span's first verse** — ``MAT 9:20`` for a row covering
      ``MAT 9:20-21`` — never a range label like ``"MAT 9:20-21"``. This is the one
      behaviour change from v3's ``/chapter``, ``/book``, ``/verse`` and ``/vrefs``,
      which return the publisher's continuation rows raw, marker text and all, and from
      v3's ``/text``, which labels a merged row with a range string. A range string is
      not a line of ``fixtures/vref.txt``, so a client joining on it drops the row
      silently — which is exactly the failure the results read was shaped to avoid.
    * **``vrefs`` is every verse the row covers**, in canonical order, beginning with
      ``vref``. A single entry in the overwhelming majority of cases; more only where the
      publisher printed several verses as one unit. Under ``include_verses=all`` it is
      always exactly ``[vref]``, because that mode does no merging.

    ``text`` never carries the stored ``<range>`` marker. Under ``union`` a merged
    continuation is not a row at all; under ``all`` it is a row with empty text. The one
    place the marker is still emitted verbatim is ``GET /v4/revisions/{id}/text``, where
    it is part of the file format rather than a leaked storage detail.
    """

    id: int | None = Field(
        default=None,
        description=(
            "The stored `verse_text` row's id, or null for a canonical verse this "
            "revision has no row for (only reachable with `include_verses=all`). On a "
            "merged span it is the anchor's row. Stable, but not a handle: no v4 "
            "endpoint addresses a single verse by id."
        ),
    )
    revision_id: int = Field(
        description="The revision this verse belongs to (echoed from the path).",
    )
    vref: str = Field(
        description=(
            "The verse this row is stored under — the **first** verse of the span when "
            "the publisher merged several. Always a literal canonical verse reference, "
            "so it joins against `vref.txt` and against an assessment's results."
        ),
    )
    vrefs: list[str] = Field(
        description=(
            "Every verse this row's text covers, in canonical order and beginning with "
            "`vref`. A single entry unless the revision merged verses into this one, in "
            "which case the continuations follow. Always exactly `[vref]` under "
            "`include_verses=all`, which does not merge."
        ),
    )
    text: str = Field(
        description=(
            "The verse text, or the merged text of the span. Empty for a canonical "
            "verse this revision has no text for. Never the `<range>` marker."
        ),
    )


class RevisionChaptersOut(V4BaseModel):
    """Response body for ``GET /v4/revisions/{id}/chapters``.

    v3's ``RevisionChapters`` carried over unchanged, wrapper object included: book
    abbreviation to the chapter numbers this revision has verses for, books in canonical
    order and chapters ascending within each.

    The wrapper is kept rather than returning the bare map because a JSON object whose
    keys are data documents as ``additionalProperties`` in OpenAPI, which generated
    clients render as an untyped bag; and because a top-level object leaves room to add
    a sibling field later without changing the response *shape*.
    """

    chapters: dict[str, list[int]] = Field(
        description=(
            "Book abbreviation to the chapter numbers this revision has verses for. "
            "Books in canonical order, chapters ascending."
        ),
    )

    model_config = {
        **V4BaseModel.model_config,
        "json_schema_extra": {
            "example": {"chapters": {"GEN": [1, 2, 3], "MAT": [1, 2]}}
        },
    }
