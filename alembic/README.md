
# Project Title

This project uses Alembic for database migrations. Follow the steps below to run a migration.

## Prerequisites

Ensure you have the following installed on your machine:

- Python
- Alembic


The database target for the migration is defined by the AQUA_DB env var, for local db it should be 

```
export AQUA_DB="postgresql://dbuser:dbpassword@localhost:5432/dbname"
```

## Running Migrations

1. To generate a new migration script, run:

```
alembic revision --autogenerate -m "my migration"
```

This will create a new migration script in your `alembic/versions` directory.

2. Edit/Audit the newly created script to include the changes you want to make to your database schema.

3. To apply the migration, run:

```
alembic upgrade head
```

This will apply all migrations up to the head (the most recent migration).

## Rolling Back Migrations

If you need to undo a migration, you can use the `downgrade` command:

```
alembic downgrade -1
```

This will undo the last batch of migrations applied.

## Production Deploy & Rollback Runbook

### Automatic migration on deploy

Pushes to the `release` branch run `.github/workflows/release.yml`. The
pipeline executes in order:

1. **tests** — lint, build the image, run the test suite against a temporary
   local Postgres.
2. **migrate** — runs `alembic upgrade head` against the production database
   using the `PROD_AQUA_DB` GitHub secret. A `concurrency` group prevents
   overlapping migration runs, so even if two commits land back-to-back only
   one migration job runs at a time.
3. **push** — builds and pushes the new image to ECR (which App Runner picks
   up). This only runs once the migration has succeeded, so the running app
   never sees a schema older than the code it shipped with.

If `migrate` fails, `push` is skipped and the previous image continues to
serve traffic against the old schema. Fix the migration on a new commit and
re-run the workflow.

### Rolling back a production migration

If a migration has gone out and needs to be reverted:

1. Take a logical backup of the affected tables first (or confirm a recent
   RDS snapshot exists) — `alembic downgrade` is destructive and migrations
   are not always perfectly reversible.
2. From a runner with `PROD_AQUA_DB` in the environment (a GitHub Actions
   workflow_dispatch job, or an authorised operator's shell), run:

   ```
   cd alembic
   alembic current             # confirm what we are rolling back from
   alembic downgrade -1        # undo the most recent migration
   alembic current             # confirm new head
   ```
3. Re-deploy the previous app image so the code matches the schema.

Never run `alembic downgrade` against production from a developer laptop
without confirming the backup first. The `AQUA_DB` env var must point at the
prod database — see the warning in the top-level README.

## Viewing Migration History

To view the history of migrations, run:

```
alembic history
```