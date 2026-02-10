# CLAUDE.md

This file contains development notes and instructions for working with this codebase.

## Running Tests Locally

### Virtual Environment

The project uses a `.venv` in the repo root. All CLI tools (`alembic`, `pytest`, etc.) are at `.venv/bin/`.

### Prerequisites

1. Check that port 5432 is free (other project containers may hold it):
   ```bash
   docker ps --filter "publish=5432"
   # If occupied, stop the conflicting container first
   ```

2. Start the PostgreSQL database with pgvector:
   ```bash
   docker compose up -d db
   ```
   If the container was previously created without port mappings, recreate it:
   ```bash
   docker compose down db && docker compose up -d db
   ```

3. Enable the pgvector extension (first time only):
   ```bash
   PGPASSWORD=dbpassword psql -h localhost -U dbuser -d dbname -c "CREATE EXTENSION IF NOT EXISTS vector;"
   ```

### Running Tests

Set the `AQUA_DB` environment variable and run pytest:

```bash
# Run all agent routes tests
AQUA_DB="postgresql+asyncpg://dbuser:dbpassword@localhost:5432/dbname" .venv/bin/python -m pytest test/test_agent_routes/test_agent_routes.py -v

# Run specific tests by keyword
AQUA_DB="postgresql+asyncpg://dbuser:dbpassword@localhost:5432/dbname" .venv/bin/python -m pytest test/test_agent_routes/test_agent_routes.py -k word_alignment -v
```

### Stopping the Database

```bash
docker compose down
```

## Alembic Migrations

### Directory Structure

Migrations are located in `alembic/migrations/versions/`. The alembic config is at `alembic/alembic.ini`.

### Checking Migration Status

**Important:** Alembic must be run from the `alembic/` subdirectory (it expects `migrations/` relative to cwd).

```bash
cd alembic

# Check current heads
AQUA_DB="postgresql+asyncpg://dbuser:dbpassword@localhost:5432/dbname" ../.venv/bin/alembic heads

# View migration history
AQUA_DB="postgresql+asyncpg://dbuser:dbpassword@localhost:5432/dbname" ../.venv/bin/alembic history

# Check current database revision
AQUA_DB="postgresql+asyncpg://dbuser:dbpassword@localhost:5432/dbname" ../.venv/bin/alembic current
```

### Running Migrations

```bash
cd alembic
AQUA_DB="postgresql+asyncpg://dbuser:dbpassword@localhost:5432/dbname" ../.venv/bin/alembic upgrade head
```

### Creating New Migrations

When creating a new migration file manually:
1. Check the current head revision with `alembic heads`
2. Set `down_revision` to point to the current head
3. Use a unique revision ID (12 character hex string)

### Validating Migration Files

```bash
AQUA_DB="postgresql+asyncpg://dbuser:dbpassword@localhost:5432/dbname" python -c "
from alembic.config import Config
from alembic.script import ScriptDirectory
cfg = Config('alembic/alembic.ini')
cfg.set_main_option('script_location', 'alembic/migrations')
script = ScriptDirectory.from_config(cfg)
rev = script.get_revision('YOUR_REVISION_ID')
print(f'Revision: {rev.revision}')
print(f'Down revision: {rev.down_revision}')
"
```

## Database Connection

The application uses async SQLAlchemy with asyncpg. The database URL must use the `postgresql+asyncpg://` scheme.

- Environment variable: `AQUA_DB`
- Default test database: `postgresql+asyncpg://dbuser:dbpassword@localhost:5432/dbname`

## Test Fixtures

Tests use `eng` (English) and `swh` (Swahili) as language codes since these are set up in the test fixtures' `iso_language` table. Other language codes (like `fra`, `deu`) will cause foreign key constraint errors.

## Pre-commit Hooks

The project uses pre-commit hooks for:
- black (code formatting)
- isort (import sorting)
- trailing whitespace removal
- end of file fixing

If a commit fails due to formatting, the hooks will auto-fix the files. Simply re-stage and commit again.
