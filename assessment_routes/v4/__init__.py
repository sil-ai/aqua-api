"""v4 assessment routers (epic #842).

Mirrors ``assessment_routes/v3/``. Route modules here are registered on the v4
sub-application in :func:`api_v4.app.create_v4_app`.

Today this holds the submit, poll, list and delete halves of the Assessments slice
(#893): ``assessment_routes.py`` (HTTP) over ``assessment_service.py`` (data access,
authorization, Modal dispatch). The typed result sub-resources and the comparisons
family land in follow-up PRs on the same issue. The runner-facing surface
(``results_push_*``, ``eflomal-*``, ``tfidf-artifacts/*``) stays on v3 by design —
it is internal plumbing, not client contract.
"""
