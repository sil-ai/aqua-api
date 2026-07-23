__version__ = "v4"

import fastapi

router = fastapi.APIRouter()


def v4_status_payload() -> dict:
    """The v4 discovery payload — the single source of truth for what ``/v4``
    reports about itself.

    Shared so the mounted sub-app's ``/v4/`` root and the parent-app bare
    ``/v4`` health-check shim (``app.py``) can never drift apart. See
    :func:`v4_root` for the meaning of each field.
    """
    return {"version": "v4", "status": "preview"}


# Only the trailing-slash root lives on the sub-app. The bare ``/v4`` form is
# handled by a hidden route on the *parent* app (``app.py``): once ``/v4`` is a
# mount, Starlette 307-redirects a request for the bare mount path to ``/v4/``
# before it can reach this router, so a bare handler here would be dead code.
@router.get("/")
async def v4_root():
    """v4 API root — the stable signal that the ``/v4`` surface is mounted.

    v4 is released **beside** a frozen v3 as an opt-in surface (epic #842);
    ``/latest`` deliberately stays pinned to v3. The individual v4 resources
    are added under this prefix by the contract issues (#825-#831). Until then
    this root is the only guaranteed ``/v4`` endpoint, so clients and health
    checks have something to hit to confirm v4 is live.

    ``status`` is ``"preview"`` while v4 is being built out; it becomes
    ``"stable"`` when the contract is finalized.
    """
    return v4_status_payload()
