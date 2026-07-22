"""v4 Pydantic schemas (issue #830, epic #842).

Home for the v4 request/response models. Kept under ``api_v4/`` — deliberately
*outside* the top-level ``schemas/`` package — so v4 schemas never leak into the
legacy ``models`` shim (``schemas/__init__.py`` re-exports every ``schemas/*``
module for the frozen v3 ``from models import X`` surface; v4 must stay off that
surface).

New v4 schemas subclass :class:`api_v4.schemas.base.V4BaseModel`. Per-domain
modules (``api_v4/schemas/bible.py`` etc.) are added by the contract issues
(#825-#831); this PR ships only the shared base.
"""
