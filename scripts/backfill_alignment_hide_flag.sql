-- Backfill NULL hide / flag in alignment_top_source_scores.
--
-- Context: issue #596. Rows pushed via POST /v3/assessment/{id}/alignment-scores
-- landed with hide=NULL because the endpoint omitted the column. Reads through
-- GET /alignmentscores then 500 because Pydantic rejects None on
-- WordAlignment.hide: bool.
--
-- The application fix in PR #597 sets hide=False on every new insert and
-- ALTER COLUMN ... SET DEFAULT false (in the accompanying migration) covers
-- any future caller that omits the column. This playbook heals the existing
-- NULL rows.
--
-- Why this is fast on a 1.7B-row table: the bug is per-assessment (the push
-- endpoint writes all rows for one assessment atomically, so within any
-- single assessment every row is either all-NULL or all-False), so we can
-- scope by `assessment.requested_time` and walk only the broken assessments
-- via the existing ix_alignment_top_source_scores_assessment_id index. No
-- full-table scan; no new index needed.
--
-- Run each step separately in psql with autocommit (default). Don't BEGIN
-- a single big transaction around the whole file -- that would defeat the
-- per-assessment lock-release benefit when running step 3b.

-- =========================================================================
-- Step 1: Count broken rows (read-only; gives you the scale of the job)
-- =========================================================================
-- Expect this to take seconds, not minutes -- it uses the assessment_id
-- index for the join. If the count is small enough to be comfortable as
-- one transaction (rule of thumb: under a few million rows), step 3a is
-- fine. If it's large, prefer step 3b.

SELECT COUNT(*) AS broken_rows
FROM alignment_top_source_scores ats
JOIN assessment a ON a.id = ats.assessment_id
WHERE a.type = 'word-alignment'
  AND a.requested_time >= DATE '2026-03-25'
  AND (ats.hide IS NULL OR ats.flag IS NULL);


-- =========================================================================
-- Step 2: List which assessments are broken (read-only; for verification)
-- =========================================================================
-- Useful sanity check: confirms the breakage is concentrated in recent
-- word-alignment assessments and shows you how many rows each one has.

SELECT a.id            AS assessment_id,
       a.requested_time,
       COUNT(*)        AS broken_rows
FROM assessment a
JOIN alignment_top_source_scores ats ON ats.assessment_id = a.id
WHERE a.type = 'word-alignment'
  AND a.requested_time >= DATE '2026-03-25'
  AND (ats.hide IS NULL OR ats.flag IS NULL)
GROUP BY a.id, a.requested_time
ORDER BY a.requested_time DESC;


-- =========================================================================
-- Step 3a: Single-UPDATE backfill (preferred when total broken rows is small)
-- =========================================================================
-- One atomic transaction. Each row touched is found via the assessment_id
-- index. Concurrent INSERTs to the table are not blocked (different rows);
-- concurrent UPDATEs to the *same* rows would block, but nothing in the
-- application updates these rows.

UPDATE alignment_top_source_scores ats
SET hide = COALESCE(ats.hide, false),
    flag = COALESCE(ats.flag, false)
FROM assessment a
WHERE ats.assessment_id = a.id
  AND a.type = 'word-alignment'
  AND a.requested_time >= DATE '2026-03-25'
  AND (ats.hide IS NULL OR ats.flag IS NULL);


-- =========================================================================
-- Step 3b: Per-assessment loop (use if step 3a's transaction would be too
-- large; e.g. tens of millions of rows or you want incremental progress)
-- =========================================================================
-- Run this in plain psql (autocommit). Each iteration of the FOR loop
-- runs in its own transaction because of the explicit COMMIT inside the
-- DO ... block (Postgres 11+).
--
-- If you'd rather not use a procedural block, you can copy the assessment
-- ids from step 2 and run a one-line UPDATE per id, e.g.:
--
--   UPDATE alignment_top_source_scores
--   SET hide = COALESCE(hide, false), flag = COALESCE(flag, false)
--   WHERE assessment_id = 17612 AND (hide IS NULL OR flag IS NULL);

-- DO blocks default to a single implicit transaction -- to commit between
-- iterations we use a procedure call. (If your psql role can't CREATE
-- PROCEDURE, fall back to the manual one-line UPDATE per id approach.)

CREATE OR REPLACE PROCEDURE backfill_alignment_hide_flag(since DATE)
LANGUAGE plpgsql
AS $$
DECLARE
  aid integer;
  cnt integer;
BEGIN
  FOR aid IN
    SELECT id FROM assessment
    WHERE type = 'word-alignment' AND requested_time >= since
    ORDER BY id
  LOOP
    UPDATE alignment_top_source_scores
    SET hide = COALESCE(hide, false),
        flag = COALESCE(flag, false)
    WHERE assessment_id = aid
      AND (hide IS NULL OR flag IS NULL);
    GET DIAGNOSTICS cnt = ROW_COUNT;
    IF cnt > 0 THEN
      RAISE NOTICE 'assessment_id=% updated=%', aid, cnt;
    END IF;
    COMMIT;
  END LOOP;
END
$$;

CALL backfill_alignment_hide_flag(DATE '2026-03-25');

DROP PROCEDURE backfill_alignment_hide_flag(DATE);


-- =========================================================================
-- Step 4: Verify (re-run step 1; should be 0)
-- =========================================================================

SELECT COUNT(*) AS still_broken
FROM alignment_top_source_scores ats
JOIN assessment a ON a.id = ats.assessment_id
WHERE a.type = 'word-alignment'
  AND a.requested_time >= DATE '2026-03-25'
  AND (ats.hide IS NULL OR ats.flag IS NULL);
-- Expect: 0
