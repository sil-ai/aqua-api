"""The v4 delta-sync watermark contract (issue #899, epic #842).

Every v4 list with a modification timestamp is also a delta feed: pass
``updated_since`` and you get only the rows written after it, soft-deletes
included. This module owns the one thing that makes such a feed *safe to trust* —
how the next watermark is computed — so that every list computes it identically
and no client has to re-derive it.

Why the watermark cannot simply be ``max(updated_at)``
-----------------------------------------------------

``updated_at`` is stamped by ``clock_timestamp()`` when the statement runs, but a
row only becomes **visible** when its transaction commits. Those are two different
instants, and between them a delta can be served that misses the row *forever*::

    t=0  txn A updates row R  ->  R.updated_at = 0, txn A stays open
    t=1  row S is updated and committed        ->  S.updated_at = 1
    t=2  a delta is served: it sees S, not R.  max(updated_at) = 1
    t=3  txn A commits; R becomes visible, still stamped 0
         the next poll asks updated_since=1  ->  R never appears again

A second, independent mechanism: ``clock_timestamp()`` is the wall clock, not a
monotonic one, so a backwards NTP step makes ``updated_at`` non-monotonic outright.

Neither mechanism can be fixed by changing what the watermark *is*. Any value
assigned inside the transaction — a timestamp, a sequence number, the row's ``xmin``
— is assigned before the commit that makes the row visible, so it has the same gap.
The only exact fixes are to order by transaction visibility, or to stamp at commit;
both were measured and rejected (see :ref:`the decision <decision>` below).

What we do instead: a server-applied safety lap
-----------------------------------------------

The server hands the client its next watermark, already lapped back by
:data:`DELTA_SAFETY_LAP`, in the ``next_updated_since`` field of the list envelope.
Re-sending a slightly stale watermark re-delivers a small overlap of rows the client
has already seen; because a mirror's upserts and deletes are idempotent, that costs
nothing and closes both gaps at once, provided the lap exceeds

* the longest write transaction that touches a delta-tracked table, and
* the largest backwards step the database host's clock can take.

The lap is set from a measurement of the first, which dominates. The longest such
transaction is the revision upload (``bible_routes.v4.revision_service.create_revision``):
the ``bible_revision`` row is flushed first — stamping ``updated_at`` — and then all
~41,899 verse rows are inserted before the single commit, so the whole upload sits
inside the gap. Measured on PostgreSQL 16 over loopback:

===============================  ==========  =================
payload                          verse rows  stamp->commit gap
===============================  ==========  =================
``fixtures/eng-eng-kjv.txt``     36,694      2.3 - 2.8 s
``MAX_TEXT_BYTES`` (52.4 MB)     41,899      4.6 s
===============================  ==========  =================

That worst case is bounded *by construction* rather than by convention: the payload
cannot exceed ``api_v4.schemas.bible.MAX_TEXT_BYTES`` (a 422 before any write), and
the row count is fixed at the vref skeleton's 41,899 slots, so a larger upload is
not a shape the endpoint accepts. The other two delta-tracked tables are nowhere
near it — ``assessment`` commits immediately after its INSERT, and every
``bible_version`` write is a single-row UPDATE.

:data:`DELTA_SAFETY_LAP` is 5 minutes: ~65x the measured ceiling, which leaves room
for the ~10-20x degradation a loaded RDS instance can add and still holds if a
future write path is an order of magnitude slower than today's worst.

.. _decision:

The two exact alternatives, and why they were rejected
------------------------------------------------------

**Order by transaction visibility** (``pg_current_snapshot()`` / ``xmin``) — the
option #899 opened with, and the one that looks correct. Rejected on measurement:

* **It can never use an index.** ``xmin`` is a system column;
  ``CREATE INDEX ... (xmin)`` fails with *"data type xid has no default operator
  class"* and the expression-index workaround fails with *"index creation on system
  columns is not supported"*. A visibility-filtered delta is therefore a mandatory
  sequential scan on every poll, permanently — 27 ms over 115k cached rows locally,
  scaling linearly and worse cold. That is the wrong cost curve for an endpoint whose
  entire purpose is being cheaper than a full fetch.
* **The obvious spelling is silently wrong.** ``xmin::text::xid8`` casts cleanly and
  is epoch-naive, so after one 32-bit wraparound every row compares as ancient and
  the delta returns nothing — the same silent-stale-mirror failure this module exists
  to prevent, with a rarer trigger and no error to notice. Correct use needs
  ``pg_visible_in_snapshot`` against a stored ``pg_snapshot``, i.e. the scan above.
* It also turns the watermark into an opaque token on a surface aimed at external
  clients, where a timestamp is something a human debugging a stale mirror can read.

**Stamp at commit instead of at statement time.** A ``DEFERRABLE INITIALLY DEFERRED``
constraint trigger fires at end-of-transaction, which would shrink the gap to
microseconds and is the genuinely correct fix for the first mechanism. It is not
available: ``set_updated_at`` sits on ``bible_version`` / ``bible_revision`` /
``assessment``, tables that frozen v3 also writes, and the freeze blocks changes to
shared DDL. It would also leave the second mechanism untouched. Worth revisiting when
v3 is retired; until then the lap covers both, which a commit-time stamp alone
would not.

The contract, stated
--------------------

Both halves are obligations, and the client's half is not optional — a mirror that
skips it will silently hold a stale copy. Stated on the endpoint too
(``updated_since``'s description) and in migration guide section 9.

The server guarantees:

1. ``next_updated_since`` is computed over **every** matching row, not just the
   returned page, so a paginating client cannot skip rows by taking one page's max.
2. It already has :data:`DELTA_SAFETY_LAP` subtracted. Send it verbatim.
3. It is derived from stored ``updated_at`` values, never from the server's own clock,
   so a stale feed can never be skipped past.
4. No write transaction touching a delta-tracked table runs longer than the lap (see
   the measurement above).

The client must:

1. Send the ``next_updated_since`` it received, **verbatim**, as the next
   ``updated_since``. Do not re-derive it from the items, and do not apply a lap of
   your own — it is already applied.
2. **Never move a stored watermark backwards.** This is what makes a backwards clock
   step at most a re-delivery instead of a permanent hole.
3. Treat ``next_updated_since: null`` as "keep the watermark you have" — it means
   nothing matched, so there is nothing to advance to.
4. **Run a periodic full reconcile** (no less often than daily). Required, not
   advisory: two things no watermark can ever carry are a *hard*-deleted row, which
   never enters any window, and a *revoked* group access, because the row leaves the
   caller's scope entirely and no delta can mention it again.
"""

from datetime import datetime, timedelta

#: How far back the server laps the watermark it hands to clients. Every
#: ``next_updated_since`` is ``max(updated_at) - DELTA_SAFETY_LAP``.
#:
#: Sized from the measured stamp->commit gap of the longest write transaction in the
#: API — 4.6 s at the ``MAX_TEXT_BYTES`` payload ceiling — with roughly two orders of
#: magnitude of headroom. See the module docstring for the measurement and for why a
#: lap (rather than an exact ordering) is the right instrument here.
#:
#: Raising this is free for correctness and costs only overlap re-delivery, which is
#: idempotent for any mirror. *Lowering* it below the longest write transaction
#: silently re-opens the hole, so it is pinned by a test.
DELTA_SAFETY_LAP = timedelta(minutes=5)


def next_watermark(max_updated_at: datetime | None) -> datetime | None:
    """Return the watermark to hand back, given the max ``updated_at`` matched.

    ``None`` in (nothing matched) gives ``None`` out, which the contract defines as
    "keep the watermark you have" rather than "start over" — advancing on an empty
    result would be indistinguishable from advancing on a *failed* one.

    ``None`` also arrives when every matched row has a NULL ``updated_at`` — legacy
    rows predating the column, which ``max()`` skips. The client then keeps doing full
    fetches, which is the safe degradation: a full fetch is always correct, just more
    expensive. (The existing mirror already treats a missing ``updated_at`` as
    non-advancing, so this matches behavior downstream rather than surprising it.)

    The lap is subtracted from the matched maximum and never from ``now()``: on a
    feed that has been quiet for a while, ``now() - lap`` would sit far ahead of the
    newest row and skip everything written in between.
    """
    if max_updated_at is None:
        return None
    return max_updated_at - DELTA_SAFETY_LAP
