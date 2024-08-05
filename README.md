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

2. Install the requirements:

    ```
    $ pip install -r requirements.txt
    ```

3. Run the API:

    ```
    $ GRAPHQL_URL=<value> AWS_ACCESS_KEY=<value> GRAPHQL_SECRET=<value> AQUA_DB=<value> AWS_SECRET_KEY=<value> uvicorn app:app --host 0.0.0.0 --port 8000
    ```

4. Use Postman or cURL to interact with (i.e., call) the various endpoints to see how they work. And/or pull up the docs at `localhost:8080/docs`

## Environment Variables

`AQUA_DB` - Specifies the database environment. **Important Note**: The `AQUA_DB` variable should never be set to `production value` in your local Bash session unless you are actively conducting a migration. Setting this variable to `production` outside of a controlled migration process can result in unintended changes to live data. Always ensure this variable is set to a development or staging value for local development.

## Using the API

To use a deployed version of the API, you will need:

- The URL of the deployed API endpoint (referred to below as `<url>`, which should be replaced by the actual URL endpoint)
- An active API key (referred to below as `<key>`, which should be replace by your actual API key)

You can review the live swagger docs of the API by visiting `<url>/docs`. This will list out the endpoints, HTTP methods, parameters, etc. that are available.

To call the API, you need to use OAuth 2.0 and set the current token for authentication to your API key. Here are some examples that show how to list versions in the API:

### cURL

```
$ curl --location --request GET '<url>/version' \
--header 'Authorization: Bearer <key>'
```

### Python - requests

```
import requests

url = "<url>"

payload={}
headers = {
  'Authorization': 'Bearer <key>'
}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)

```


## Integration Notebook

This notebook helps to test the flow of the API, using different calls you would usually do,
as an admin and as a regular user, to be able to use it please do the following:

1. Build the db
```
make localdb-up
```
2. Populate the db with conftest(you have to run the following commands on the same shell),
this step is necessary for you to have data to run the API calls in the notebook.
```
export PYTHONPATH=$PYTHONPATH:$(pwd)
conftest.py
```

3. Run the API
```
uvicorn app:app --host 0.0.0.0 --port 8000
```