"""v4 agent routers (epic #842).

Mirrors ``agent_routes/v3/``. Route modules here are registered on the v4
sub-application in :func:`api_v4.app.create_v4_app`.

Today this holds the two agent-result reads (#896): ``agent_routes.py`` (HTTP) over
``agent_service.py`` (queries). They hang off ``/v4/assessments/{id}/…`` because guide
§15.7 rules that critique issues and agent translations *are* assessment results, so the
router shares the ``/assessments`` prefix with the Assessments router while declaring
only sub-paths — see ``agent_routes.py`` for why that is safe.

Still to land on the same issue: the resolution ``PATCH``, and then ``/v4/lexeme-cards``
and ``/v4/agent-word-alignments``, which are version- and language-keyed reference data
with no assessment to nest under and so become top-level collections here.

The five agent **writes** stay on v3 by design (§15.7): ``POST /agent/critique``,
``/agent/translation``, ``/agent/translations``, ``/agent/word-alignment`` and its
``/bulk`` are the runner storing its own output, not client contract — the same class as
the ``results_push_*`` endpoints. ``POST /agent/translations-test`` is a debug leftover
that echoes the size of the body you send it, and is not carried either.
"""
