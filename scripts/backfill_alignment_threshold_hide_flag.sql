-- Backfill NULL hide / flag in alignment_threshold_scores.
--
-- Sister playbook to scripts/backfill_alignment_hide_flag.sql, which
-- handled alignment_top_source_scores. Same root cause: the producer
-- (the assessment runner) inserts rows without setting hide / flag, so
-- they land NULL and reads through models.WordAlignment 500 under
-- Pydantic v2.
--
-- The application fix in this PR adds default=False / server_default
-- 'false' on the model and the migration sets DEFAULT false on the
-- columns, so any future insert that omits these columns picks up
-- False. This playbook heals existing NULL rows.
--
-- Difference from the top-source playbook: there is no known landing
-- date for the bug on this table -- nothing in aqua-api writes here, so
-- the producer is external and has likely always written NULL. We
-- therefore do NOT date-bound the scan; we walk every word-alignment
-- assessment. The accompanying migration adds
-- ix_alignment_threshold_scores_assessment_id, which is what makes the
-- per-assessment loop in step 3b cheap (index seek per assessment); if
-- you are running this on a database that has not yet picked up that
-- migration, prefer step 3a -- step 3b will seq-scan the threshold
-- table once per assessment without the index.
--
-- Run each step separately in psql with autocommit (default).

-- =========================================================================
-- Step 1: Count broken rows (read-only)
-- =========================================================================

SELECT COUNT(*) AS broken_rows
FROM alignment_threshold_scores ats
JOIN assessment a ON a.id = ats.assessment_id
WHERE a.type = 'word-alignment'
  AND (ats.hide IS NULL OR ats.flag IS NULL);


-- =========================================================================
-- Step 2: List which assessments are broken (read-only)
-- =========================================================================

SELECT a.id            AS assessment_id,
       a.requested_time,
       COUNT(*)        AS broken_rows
FROM assessment a
JOIN alignment_threshold_scores ats ON ats.assessment_id = a.id
WHERE a.type = 'word-alignment'
  AND (ats.hide IS NULL OR ats.flag IS NULL)
GROUP BY a.id, a.requested_time
ORDER BY a.requested_time DESC;


-- =========================================================================
-- Step 3a: Single-UPDATE backfill (preferred when total broken rows is small)
-- =========================================================================

UPDATE alignment_threshold_scores ats
SET hide = COALESCE(ats.hide, false),
    flag = COALESCE(ats.flag, false)
FROM assessment a
WHERE ats.assessment_id = a.id
  AND a.type = 'word-alignment'
  AND (ats.hide IS NULL OR ats.flag IS NULL);


-- =========================================================================
-- Step 3b: Per-assessment loop (use if step 3a's transaction would be too
-- large; e.g. tens of millions of rows or you want incremental progress)
-- =========================================================================
-- Run this in plain psql (autocommit). Each iteration of the FOR loop
-- runs in its own transaction because of the explicit COMMIT inside the
-- procedure body (Postgres 11+).
--
-- If your psql role can't CREATE PROCEDURE, fall back to copying ids
-- from step 2 and running one UPDATE per id, e.g.:
--
--   UPDATE alignment_threshold_scores
--   SET hide = COALESCE(hide, false), flag = COALESCE(flag, false)
--   WHERE assessment_id = 17612 AND (hide IS NULL OR flag IS NULL);

CREATE OR REPLACE PROCEDURE backfill_threshold_hide_flag()
LANGUAGE plpgsql
AS $$
DECLARE
  aid integer;
  cnt integer;
BEGIN
  FOR aid IN
    SELECT id FROM assessment
    WHERE type = 'word-alignment'
    ORDER BY id
  LOOP
    UPDATE alignment_threshold_scores
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

CALL backfill_threshold_hide_flag();

DROP PROCEDURE backfill_threshold_hide_flag();


-- =========================================================================
-- Step 4: Verify (re-run step 1; should be 0)
-- =========================================================================

SELECT COUNT(*) AS still_broken
FROM alignment_threshold_scores ats
JOIN assessment a ON a.id = ats.assessment_id
WHERE a.type = 'word-alignment'
  AND (ats.hide IS NULL OR ats.flag IS NULL);
-- Expect: 0
