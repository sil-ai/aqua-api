"""v4 auth / users / groups routers and services (epic #842).

New package rather than edits to the flat ``security_routes/`` modules beside it:
``auth_routes.py`` and ``admin_routes.py`` are shared, version-agnostic infra —
``get_current_user`` is imported by 16 v3 route modules — so v4 *adds* here instead of
fork-editing them.

On the v4 side, :mod:`security_routes.v4.dependencies` is the only module that imports
``get_current_user`` at all: it wraps it as ``get_current_user_v4``, which is what
:mod:`api_v4.app` wires in for router-level auth. That indirection exists so v4
publishes its own ``tokenUrl`` (#928) — see that module for the full reason.

There is deliberately no sibling ``security_routes/v3/``: the flat modules *are*
v3 and moving them would break every one of those imports. That asymmetry with
``bible_routes/`` (which does have ``v3/`` and ``v4/``) is intentional.
"""
