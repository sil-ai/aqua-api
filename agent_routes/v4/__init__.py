"""v4 agent routers (epic #842).

Mirrors ``agent_routes/v3/``. Route modules here are registered on the v4
sub-application in :func:`api_v4.app.create_v4_app`.

Today this holds the agent-result slice (#896): ``agent_routes.py`` (HTTP) over
``agent_service.py`` (queries and the one write). Two reads and the resolution ``PATCH``,
all hanging off ``/v4/assessments/{id}/…`` because guide §15.7 rules that critique issues
and agent translations *are* assessment results — so the router shares the
``/assessments`` prefix with the Assessments router while declaring only sub-paths; see
``agent_routes.py`` for why that is safe.

Also here, on the same issue: ``lexeme_card_routes.py`` over ``lexeme_card_service.py``,
serving ``/v4/lexeme-cards``. Version- and language-keyed reference data with no
assessment to nest under, so a top-level collection rather than a sub-resource, and its
own router and service because it shares no authorization with the two reads above.

``/v4/agent-word-alignments`` was **not** built. §15.7 planned it, and a caller check
before starting reversed that: no client repo reads ``GET /agent/word-alignment`` or its
``/all`` form, and the table is a cache from the retired NLLB pipeline that nothing writes
any more — ``aqua-assessments`` withdrew its own bulk push as redundant once eflomal
results were queryable in their own right. The two reads stay on v3, on the same "no
caller in any local client repo" ruling that made tokenizer and pivot v3-only.

The five agent **writes** stay on v3 by design (§15.7): ``POST /agent/critique``,
``/agent/translation``, ``/agent/translations``, ``/agent/word-alignment`` and its
``/bulk`` are the runner storing its own output, not client contract — the same class as
the ``results_push_*`` endpoints. ``POST /agent/translations-test`` is a debug leftover
that echoes the size of the body you send it, and is not carried either.
"""
