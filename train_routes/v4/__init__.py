"""v4 train routers (epic #842).

Mirrors ``train_routes/v3/``. Route modules here are registered on the v4
sub-application in :func:`api_v4.app.create_v4_app`.

This holds the whole Training slice (#895): ``train_routes.py`` (HTTP, two routers —
``/training-sessions`` and ``/training-jobs``) over ``train_service.py`` (authorization,
dispatch, and the interleaved per-verse result queries). ``GET /train/{job_id}/data``
stays on v3 by design — it is the runner's data-pull, not client contract.
"""
