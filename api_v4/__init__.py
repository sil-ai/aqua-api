"""The v4 API surface (epic #842).

``api_v4`` owns everything specific to the ``/v4`` sub-application:

* :mod:`api_v4.app` — the ``create_v4_app`` factory; the main app mounts its
  result at ``/v4``.
* :mod:`api_v4.meta_routes` — the ``/v4/`` discovery root.
* :mod:`api_v4.schemas` — v4 Pydantic schemas (shared ``V4BaseModel`` base).

Domain routers live under the ``<domain>_routes/v4/`` convention (mirroring
``<domain>_routes/v3/``) and are registered on the sub-app in
:func:`api_v4.app.create_v4_app`.
"""
