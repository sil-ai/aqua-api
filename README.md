# AQuA API

## Schema

(This is a WIP schema, but PRs should generally conform to this schema. If a PR deviates, it should update this section of the README and explicitly give the logic for deviating from the schema below.)

✔️ - currently implemented as expected

❌ - not yet implemented, or not yet conforming to this schema

- `/version`
    - ✔️ `GET` - lists versions
    - ✔️ `POST` - create a new version
    - ❌ `PUT` - updates a version metadata
    - ✔️ `DELETE` - removes a version (WARNING - and could either delete all child data in the graph or leave branches without a trunk)
- `/revision`
    - ✔️ `GET` - lists revisions
    - ✔️ `POST` - uploads Bible data to create a new revision
    - ✔️ `DELETE` - removes a revision and the downstream `verse_text` data
- `/assessment`
    - ✔️ `GET` - lists assessments
    - ✔️ `POST` - triggers a new assessment (regardless of assessment type)
    - ✔️ `DELETE` - removes an assessment (and downstream result data from the database)
- `/result`
    - ✔️ `GET` - retrieves results of assessments from the database

## Developing the API

To run the API locally while developing:

1. Get the necessary DB creds, etc.
2. Clone the repository and enter the folder.
    ```
    $ git clone https://github.com/sil-ai/aqua-api.git
    $ cd aqua-api
    ```

3. Install the requirements:

    ```
    $ pip install -r requirements.txt
    ```

4. In case you face an error on the previous point regarding a postgres package.
    ```
    $ sudo apt get install libpq-dev
    ```

3. Run the API (make sure tu have the .env file in the directory):

    ```
    $ make build-local
    $ make up
    ```

4. Use Postman or cURL to interact with (i.e., call) the various endpoints to see how they work. And/or pull up the docs at `localhost:8000/docs`.

## Environment file

The environment file should have the following variables:

- IMAGENAME
- REGISTRY
- AQUA_DB
- AQUA_DB_SYNC
- AQUA_URL
- GRAPHQL_SECRET
- GRAPHQL_URL
- AQUA_API_KEY
- API_KEY
- KEY_VAULT
- AWS_ACCESS_KEY
- AWS_SECRET_KEY
- TEST_KEY
- MODAL_WEBHOOK_TOKEN
- ADMIN_PASSWORD
- TEST_USER
- TEST_PASSWORD

## Swagger

Once you put the API app, you will find detailed documentation on `localhost:8080/docs`.
On the top right corner you will see a button named `Authorize`. There, in the segment
`OAuth2PasswordBearer`, you will be able to put your username and password to test the API.

## Makefile

A Makefile is a tool to help automize repetitive tasks you may want to perform with
the API, here are the most important commands.

1. `make localdb-up`

Builds the database locally through Docker, if you want to use this local db you have to
reference it through the corresponding env variable.

The local DB will be accesible at port 5432, with a user: dbuser, a password: dbpassword, and
a database: dbname, therefore the `AQUA_DB` varible would be set to postgresql+asyncpg://dbuser:dbpassword@localhost:5432/dbname.

2. `make build-local`

Builds a docker image witn the current code, run it before you run `make up`, to see new changes.

3. `make up`

This will build all the project, including the API and the DB, through Docker as well.

The APIi woud be accessible through 8000 port

4. `make down`

This will tear down all the project, API and DB, stopping the Docker containers.

5. `make test`

This will run the tests using pytest, creating a database, or using an existing one.
If you were using a local db is important that you perform make down before performing
this command, because in the moment it starts to populate the db, it will find
duplicates and raise an error.

6. `make linting`

This will perform a formatting with the tool black, and a linting verification with flake8

7. Other commands like push-branch are used on GitHub workflows, to push
to the runners, but this are used automatically when you push to main.


## Environment Variables

`AQUA_DB` - Specifies the database environment. **Important Note**: The `AQUA_DB` variable should never be set to `production value` in your local Bash session unless you are actively conducting a migration. Setting this variable to `production` outside of a controlled migration process can result in unintended changes to live data. Always ensure this variable is set to a development or staging value for local development.


## Integration Notebook

This notebook helps to test the flow of the API, using different calls you would usually do,
as an admin and as a regular user, to be able to use it please do the following:

1. Build the db
```
$ make localdb-up
```
2. Populate the db with conftest(you have to run the following commands on the same shell),
this step is necessary for you to have data to run the API calls in the notebook.
```
$ export PYTHONPATH=$PYTHONPATH:$(pwd)
$ conftest.py
```

3. Run the API
```
$ make build-local
$ make up
```