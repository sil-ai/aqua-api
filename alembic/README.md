
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

## Viewing Migration History

To view the history of migrations, run:

```
alembic history
```