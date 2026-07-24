import importlib
import os

import sqlalchemy.ext.asyncio as sa_async

import config
import database.dependencies as deps


def test_pooled_engine_wires_statement_timeout(monkeypatch):
    """Regression guard for the #836 revert.

    PR #656 fixed QueuePool exhaustion by (a) raising the pool defaults and
    (b) passing a server-side ``statement_timeout`` through asyncpg
    ``connect_args`` — the real safety net that stops one runaway query from
    pinning a pooled connection and cascading into 500s. The typed-config
    refactor (#836) branched from a stale base and silently dropped both.

    ``test_config.py`` now covers the defaults; this asserts the *engine
    construction path itself* still wires ``connect_args`` on the pooled
    branch. conftest forces ``AQUA_DB_POOLCLASS=null`` for the whole suite, so
    we flip to the pooled branch, spy on ``create_async_engine``, reload the
    module, then restore the real NullPool engine so later test modules (which
    drive the app over TestClient's per-request event loops) are unaffected.
    """
    real_cae = sa_async.create_async_engine
    real_settings = config.settings

    captured = {}

    def spy(url, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.delenv("AQUA_DB_POOLCLASS", raising=False)
    monkeypatch.setattr(sa_async, "create_async_engine", spy)
    monkeypatch.setattr(config, "settings", config.Settings())

    try:
        importlib.reload(deps)

        kwargs = captured["kwargs"]
        assert kwargs["pool_size"] == config.settings.aqua_db_pool_size
        assert kwargs["max_overflow"] == config.settings.aqua_db_max_overflow
        server_settings = kwargs["connect_args"]["server_settings"]
        assert server_settings["statement_timeout"] == str(
            config.settings.aqua_db_statement_timeout_ms
        )
    finally:
        sa_async.create_async_engine = real_cae
        config.settings = real_settings
        os.environ["AQUA_DB_POOLCLASS"] = "null"
        importlib.reload(deps)
