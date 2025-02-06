import os

import requests

hasura_headers = {"x-hasura-admin-secret": os.getenv("NEW_HASURA_SECRET")}
db_string = os.getenv("AQUA_DB")
neon_api_key = "Bearer " + os.getenv("NEON_API_KEY")
new_url = os.getenv("NEW_HASURA_URL")

db_empty_string = db_string.split("@")[0]

if new_url[-3::] == "app":
    db_con_url = new_url + "/v1/metadata"
else:
    url_split = new_url.split("-")
    url_split[2] = (url_split[2])[0:13]
    root_url = "-".join(url_split)
    db_con_url = root_url + "/v1/metadata"

neon_api = (
    "https://console.neon.tech/api/v2/projects/" + os.getenv("NEON_DB_ID") + "/branches"
)

neon_headers = {"Authorization": neon_api_key}

neon_payload = {
    "endpoints": [{"type": "read_write"}],
    "branch": {"parent_id": os.getenv("NEON_TEMPLATE_BRANCH")},
}

new_branch_call = requests.post(neon_api, json=neon_payload, headers=neon_headers)

new_branch_endpoint = new_branch_call.json()["endpoints"][0]["host"]
new_db_conn = db_empty_string + "@" + new_branch_endpoint + "/aqua"

db_con = {
    "type": "pg_add_source",
    "args": {
        "name": "default",
        "configuration": {
            "connection_info": {
                "database_url": new_db_conn,
                "pool_settings": {
                    "retries": 1,
                    "idle_timeout": 180,
                    "max_connections": 50,
                },
            }
        },
    },
}


db_con_response = requests.post(db_con_url, json=db_con, headers=hasura_headers)

print(new_db_conn)
