-- Re-point orphaned critique issues at the latest translation for their verse.
--
-- Context: agent_critique_issue.agent_translation_id names the translation a
-- finding was judged against. The agent's verify pass rewrites the back
-- translation on some verses and persists that correction as a NEW
-- agent_translations version (the bulk endpoint auto-increments; there is no
-- PATCH route) -- and that post ran AFTER the critique post, so critiques on a
-- corrected verse were left pointing at the now-superseded row.
--
-- That pointer decides visibility. GET /agent/translations returns only the
-- latest version per vref (MAX(version) subquery), and the AQuA frontend keeps
-- only those critiques whose agent_translation_id is among the rows it loaded
-- (AgentSection.js filteredCritiqueData). A critique on a superseded row is
-- therefore dropped in the browser -- silently, at every severity, and
-- invisibly from the API side.
--
-- The application fix in sil-ai/aqua-assessments#465 persists the correction
-- and re-maps the IDs before posting critiques, so no NEW row is orphaned.
-- This playbook heals the rows already written.
--
-- Run AFTER #465 is deployed. If this runs first, assessments in the gap
-- re-orphan their own critiques; the statements are idempotent, so the remedy
-- is simply to run step 3 again.
--
-- Scope measured on prod 2026-09-11: of 15,643 critique issues, 1,541 across
-- 119 assessments point at a superseded row. This repairs 1,379 of them --
-- those whose critique was written at or before the translation row it is
-- re-pointed to, which is the ordering the bug produces.
--
-- The other 162 are deliberately left alone: their critique was written AFTER
-- the row they would be re-pointed to already existed, which the verify-pass
-- ordering cannot produce. Something else wrote them (a retried assess()
-- re-posting translations mid-run is the leading hypothesis -- Modal runs it
-- with retries=3 -- but that is unconfirmed). Re-pointing a finding whose
-- provenance we do not understand is not a repair. Related: orphans span
-- 2026-04 to 2026-09 but the verify pass only landed in 2026-07, so the 582
-- rows from April-June had some earlier cause that has not been identified.
--
-- Matching stays scoped to the critique's OWN assessment, so this never
-- re-points a finding onto a different assessment's translation -- a later
-- rerun legitimately supersedes an earlier assessment's findings, and that
-- behaviour is left alone.
--
-- Run each step separately in psql with autocommit (default).


-- =========================================================================
-- Step 1: Count the rows this will touch (read-only; confirms the scale)
-- =========================================================================
-- Expect 1379 as of 2026-09-11. A materially different number means the
-- population has moved since it was measured -- stop and re-check before
-- writing anything.

SELECT COUNT(*) AS repairable_rows
FROM agent_critique_issue AS c
JOIN (
    SELECT DISTINCT ON (assessment_id, vref)
           id, assessment_id, vref, created_at
    FROM agent_translations
    ORDER BY assessment_id, vref, version DESC
) AS t
  ON c.assessment_id = t.assessment_id
 AND c.vref = t.vref
WHERE c.agent_translation_id IS DISTINCT FROM t.id
  AND c.created_at <= t.created_at;


-- =========================================================================
-- Step 2: The excluded rows (read-only; the 162 this deliberately skips)
-- =========================================================================
-- Same orphan condition, opposite ordering. These are NOT repaired here.
-- Worth eyeballing before step 3 so the exclusion is a decision, not a
-- surprise.

SELECT c.assessment_id, COUNT(*) AS skipped_rows
FROM agent_critique_issue AS c
JOIN (
    SELECT DISTINCT ON (assessment_id, vref)
           id, assessment_id, vref, created_at
    FROM agent_translations
    ORDER BY assessment_id, vref, version DESC
) AS t
  ON c.assessment_id = t.assessment_id
 AND c.vref = t.vref
WHERE c.agent_translation_id IS DISTINCT FROM t.id
  AND c.created_at > t.created_at
GROUP BY c.assessment_id
ORDER BY skipped_rows DESC;


-- =========================================================================
-- Step 3: Snapshot the current pointers, so this is reversible
-- =========================================================================
-- The UPDATE overwrites the FK in place and the prior value is recorded
-- nowhere else. Take the snapshot BEFORE step 4. Safe to re-run: the primary
-- key plus ON CONFLICT DO NOTHING keeps the first (pre-change) value.

CREATE TABLE IF NOT EXISTS agent_critique_issue_translation_fk_backup (
    critique_issue_id        INTEGER PRIMARY KEY,
    old_agent_translation_id INTEGER NOT NULL,
    backed_up_at             TIMESTAMP NOT NULL DEFAULT now()
);

INSERT INTO agent_critique_issue_translation_fk_backup
            (critique_issue_id, old_agent_translation_id)
SELECT c.id, c.agent_translation_id
FROM agent_critique_issue AS c
JOIN (
    SELECT DISTINCT ON (assessment_id, vref)
           id, assessment_id, vref, created_at
    FROM agent_translations
    ORDER BY assessment_id, vref, version DESC
) AS t
  ON c.assessment_id = t.assessment_id
 AND c.vref = t.vref
WHERE c.agent_translation_id IS DISTINCT FROM t.id
  AND c.created_at <= t.created_at
ON CONFLICT (critique_issue_id) DO NOTHING;

-- Should match step 1.
SELECT COUNT(*) AS rows_snapshotted FROM agent_critique_issue_translation_fk_backup;


-- =========================================================================
-- Step 4: Re-point the critiques
-- =========================================================================
-- One atomic transaction, ~1.4k rows, no schema change and no lock beyond the
-- row locks. The FK is ON DELETE CASCADE and every target is an existing
-- translation for the same assessment and vref, so no reference is broken.
-- Idempotent: re-running is a no-op once rows point at the max-version row.

UPDATE agent_critique_issue AS c
SET agent_translation_id = t.id
FROM (
    SELECT DISTINCT ON (assessment_id, vref)
           id, assessment_id, vref, created_at
    FROM agent_translations
    ORDER BY assessment_id, vref, version DESC
) AS t
WHERE c.assessment_id = t.assessment_id
  AND c.vref = t.vref
  AND c.agent_translation_id IS DISTINCT FROM t.id
  AND c.created_at <= t.created_at;


-- =========================================================================
-- Step 5: Verify (read-only)
-- =========================================================================
-- Step 1's query should now return 0. The 162 from step 2 remain, which is
-- expected.

SELECT COUNT(*) AS still_repairable
FROM agent_critique_issue AS c
JOIN (
    SELECT DISTINCT ON (assessment_id, vref)
           id, assessment_id, vref, created_at
    FROM agent_translations
    ORDER BY assessment_id, vref, version DESC
) AS t
  ON c.assessment_id = t.assessment_id
 AND c.vref = t.vref
WHERE c.agent_translation_id IS DISTINCT FROM t.id
  AND c.created_at <= t.created_at;

-- Spot-check the Fwe Acts 9-12 runs that surfaced this (expect 0 rows).
SELECT c.assessment_id, COUNT(*) AS orphaned
FROM agent_critique_issue AS c
JOIN (
    SELECT DISTINCT ON (assessment_id, vref) id, assessment_id, vref
    FROM agent_translations
    ORDER BY assessment_id, vref, version DESC
) AS t
  ON c.assessment_id = t.assessment_id AND c.vref = t.vref
WHERE c.agent_translation_id IS DISTINCT FROM t.id
  AND c.assessment_id IN (32481, 32482, 32483, 32484)
GROUP BY c.assessment_id
ORDER BY c.assessment_id;


-- =========================================================================
-- Step 6: Once satisfied, drop the snapshot
-- =========================================================================
-- To undo instead, run this BEFORE dropping:
--   UPDATE agent_critique_issue AS c
--   SET agent_translation_id = b.old_agent_translation_id
--   FROM agent_critique_issue_translation_fk_backup AS b
--   WHERE c.id = b.critique_issue_id;
--
-- DROP TABLE agent_critique_issue_translation_fk_backup;
