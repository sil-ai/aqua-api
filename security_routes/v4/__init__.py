"""v4 auth / users / groups routers and services (epic #842).

New package rather than edits to the flat ``security_routes/`` modules beside it:
``auth_routes.py`` and ``admin_routes.py`` are shared, version-agnostic infra —
``get_current_user`` is imported by :mod:`api_v4.app` for router-level auth and by
16 v3 route modules — so v4 *adds* here instead of fork-editing them.

There is deliberately no sibling ``security_routes/v3/``: the flat modules *are*
v3 and moving them would break every one of those imports. That asymmetry with
``bible_routes/`` (which does have ``v3/`` and ``v4/``) is intentional.
"""
