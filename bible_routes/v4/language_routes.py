"""v4 reference lists — languages and scripts (issue #951, epic #842).

The two static code tables a client needs before it can create anything:

* ``GET /v4/languages``  — the ISO 639-3 language codes, paginated.
* ``GET /v4/scripts``    — the ISO 15924 script codes, paginated.

``POST /v4/versions`` requires ``iso_language`` and ``iso_script``, and until these
landed there was no v4 way to discover the valid values — a new client had to call v3
or already know them. That is what makes two reads over two-column tables worth their
own slice: the hole was in the *first* call a new client makes.

**Why this file lives in** ``bible_routes/v4/`` **rather than** ``api_v4/``. Both were
defensible. ``api_v4/`` already holds ``meta_routes.py``, and these are reference data
rather than Bible data, so a case exists for putting them beside it. Against that:
``api_v4/`` is otherwise the surface's *infrastructure* — the error contract,
pagination, jobs, delta, schemas — plus one public discovery root, while every
authenticated domain router follows the ``<domain>_routes/v4/`` convention that
:func:`api_v4.app.create_v4_app` documents. These endpoints are authenticated domain
routes, so putting them in ``api_v4/`` would be the first exception to both patterns.
The tie-break is what the tables are *for*: their only role in this API is to supply
``bible_version.iso_language`` and ``bible_version.iso_script``, which is why v3 put
them in ``bible_routes/v3/language_routes.py`` too. Mirroring v3's location is the
low-surprise answer and it costs nothing.

**One router, and it carries no prefix.** ``/languages`` and ``/scripts`` share no path
prefix, unlike every other v4 router, so the choice was one prefixless router declaring
both full paths or two routers with one endpoint each. One router with
``tags=["Reference"]`` groups them in ``/v4/docs`` the way a reader expects — two tags
holding one operation apiece would read as two unrelated features rather than as the
reference data they both are. The parent registers it in the same ``for domain_router``
loop as everything else, so it inherits router-level auth and the shared ``responses=``.

**Authenticated, like every other v4 domain route (#831).** Exempting them was
considered: reference data is not sensitive, v4 already exempts ``POST /v4/token`` and
the discovery root, and a brand-new client wants these codes early. It buys nothing —
a client needs a token to call anything else on the surface, including the
``POST /v4/versions`` these codes are *for* — so an exemption would widen the
unauthenticated surface in exchange for saving nobody a step. v3 gates both of these
with ``get_current_user`` and v4 keeps that.

**No error handling, and none missing.** Neither route catches anything, because
neither can raise anything: no id is looked up, so there is no 404; nothing is owned,
so there is no 403; nothing is written, so there is no 409. What remains is the
framework's own 401 and 422, both already declared by the shared ``responses=``. See
:mod:`bible_routes.v4.language_service`.
"""

__version__ = "v4"

import fastapi
from fastapi import Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from api_v4.pagination import ReferencePaginationParams, V4Page
from api_v4.schemas.bible import (
    MAX_REFERENCE_QUERY_LENGTH,
    LanguageOut,
    ScriptOut,
)
from bible_routes.v4 import language_service
from database.dependencies import get_db

router = fastapi.APIRouter(tags=["Reference"])

#: What ``q`` does, written once because both routes take exactly the same filter and a
#: description that drifted between them would document two behaviours the shared
#: service does not have.
_Q_DESCRIPTION = (
    "Optional filter. Case-insensitive; keeps a row whose **code or name** contains "
    "this text, so `swh` finds Swahili by its code and `alban` finds every language "
    "whose name mentions Albanian. Matching is a plain substring, so it over-matches "
    "on purpose — `eng` also finds Bengali — and `%` or `_` in the term match "
    "themselves rather than acting as wildcards. `total` is the number of rows that "
    "matched, not the size of the table. Surrounding whitespace is trimmed, and a "
    "blank filter is the same as none."
)


def _q_param():
    """Build the ``q`` query parameter.

    A factory rather than one shared ``Query`` instance assigned as both defaults,
    which is what this was first written as. FastAPI *mutates* the ``FieldInfo`` it is
    given while building a route — ``field_info.alias = alias`` in
    ``fastapi/dependencies/utils.py`` — so a single instance used by two routes is
    written to twice. It happens to be harmless here, because both parameters are
    named ``q`` and so both writes store the same alias; it stops being harmless the
    moment someone renames the parameter on one of the two routes, at which point
    whichever router FastAPI processes last decides the name *both* of them publish.
    That is a wrong ``/v4/openapi.json`` rather than an error, so nothing would catch
    it. A fresh instance per route cannot have the problem.
    """
    return Query(
        None,
        max_length=MAX_REFERENCE_QUERY_LENGTH,
        description=_Q_DESCRIPTION,
    )


@router.get("/languages", response_model=V4Page[LanguageOut])
async def list_languages(
    q: str | None = _q_param(),
    page: ReferencePaginationParams = Depends(),
    db: AsyncSession = Depends(get_db),
) -> V4Page[LanguageOut]:
    """List the ISO 639-3 language codes, ordered by code, paginated.

    The ``iso639`` of any item here is a value ``POST /v4/versions`` accepts as
    ``iso_language``.

    **An unparameterized call returns the whole list**, which is the one place these
    two endpoints diverge from the rest of the v4 surface: their ``limit`` defaults to
    its own maximum, because the reason to call them is to populate a picker and the
    list is bounded by ISO 639-3 rather than by how much the deployment has been used.
    ``limit``/``offset`` still page for a client that wants them. See
    :class:`api_v4.pagination.ReferencePaginationParams`.

    Replaces v3 ``GET /language``, which was registered on ``/v3`` *and* ``/latest``
    and returned an unbounded array. Two changes beyond the envelope: ``name`` is
    nullable on the wire (see :class:`api_v4.schemas.bible.ScriptOut` for why v3's
    required ``name`` is a latent 500), and ``q`` is new.
    """
    languages, total = await language_service.list_languages(
        db, limit=page.limit, offset=page.offset, q=q
    )
    items = [LanguageOut.model_validate(language) for language in languages]
    return V4Page[LanguageOut].create(items=items, total=total, pagination=page)


@router.get("/scripts", response_model=V4Page[ScriptOut])
async def list_scripts(
    q: str | None = _q_param(),
    page: ReferencePaginationParams = Depends(),
    db: AsyncSession = Depends(get_db),
) -> V4Page[ScriptOut]:
    """List the ISO 15924 script codes, ordered by code, paginated.

    The ``iso15924`` of any item here is a value ``POST /v4/versions`` accepts as
    ``iso_script``.

    Paged exactly like ``GET /v4/languages``, down to the default that returns
    everything — see there. Replaces v3 ``GET /script``, on the same terms.
    """
    scripts, total = await language_service.list_scripts(
        db, limit=page.limit, offset=page.offset, q=q
    )
    items = [ScriptOut.model_validate(script) for script in scripts]
    return V4Page[ScriptOut].create(items=items, total=total, pagination=page)
