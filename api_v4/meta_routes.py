__version__ = "v4"

import fastapi

router = fastapi.APIRouter()


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
    return {"version": "v4", "status": "preview"}
