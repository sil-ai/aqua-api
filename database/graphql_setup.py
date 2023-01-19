import requests
import os

import psycopg2


headers = {"x-hasura-admin-secret": os.getenv("GRAPHQL_SECRET")}
new_headers = {"x-hasura-admin-secret": os.getenv("NEW_HASURA_SECRET")}
db_conn = os.getenv("NEW_DB")
new_db_conn = os.getenv("NEW_DB") + "/" + new_db_name

migrate_url = os.getenv("GRAPHQL_URL")
if migrate_url[-3::] == "app":
    sql_fetch_url = migrate_url + "/v1alpha1/pg_dump"
else:
    url_split = migrate_url.split("-")
    url_split[2] = (url_split[2])[0:13]
    root_url = "-".join(url_split)

    sql_fetch_url = root_url + "/v1alpha1/pg_dump"


new_url = os.getenv("NEW_HASURA_URL")
if new_url[-3::] == "app":
    db_con_url = new_url + "/v1/metadata"
    sql_url = new_url + "/v2/query"
else:
    url_split = new_url.split("-")
    url_split[2] = (url_split[2])[0:13]
    root_url = "-".join(url_split)
    
    db_con_url = root_url + "/v1/metadata"
    sql_url = root_url + "/v2/query"


payload = {
        "opts": ["-O", "--inserts", "--schema-only"], 
        "clean_output": True, 
        "source": os.getenv("DB_NAME")
        }

sql_response = requests.post(sql_fetch_url, json=payload, headers=headers)

con = psycopg2.connect(db_conn)

db_con = {
    "type": "pg_add_source",
    "args": {
        "name": new_db_name,
        "configuration": {
          "connection_info": {
            "database_url": new_db_conn,
                "pool_settings": {
                    "retries": 1,
                    "idle_timeout": 180,
                    "max_connections": 50
                }
              }
            }
          }
        }


sql_query = {
    "type": "run_sql", 
    "args": {
        "source": "default", 
        "sql": sql_response.text
        }
    }


db_con_response = requests.post(db_con_url, json=db_con, headers=new_headers)
sql_response = requests.post(sql_url, json=sql_query, headers=new_headers)
