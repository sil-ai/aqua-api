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
    - ❌ `DELETE` - removes a revision and the downstream `verseText` data
- `/assessment`
    - ❌ `GET` - lists assessments
    - ❌ `POST` - triggers a new assessment (regardless of assessment type)
    - ❌ `DELETE` - removes an assessment (and downstream result data from the database)
- `/result`
    - ❌ `GET` - retrieves results of assessments from the database

## Local development

To run the API locally:

1. Get the necessary DB creds, etc.

2. Install the requirements:

    ```
    $ pip install -r requirements.txt
    ```
    
3. Run the API:

    ```
    $ GRAPHQL_URL=<value> AWS_ACCESS_KEY=<value> GRAPHQL_SECRET=<value> AQUA_DB=<value> AWS_SECRET_KEY=<value> uvicorn app:app --host 0.0.0.0 --port 8000
    ```
    
4. Use Postman or cURL to interact with (i.e., call) the various endpoints to see how they work. And/or pull up the docs at `localhost:8080/docs`
