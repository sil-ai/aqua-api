import json
import requests
import os

from database_setup import sql_query, db_connection


headers = {"x-hasura-admin-secret": os.getenv("HASURA_SECRET")}

db_con_url = os.getenv("HASURA_URL") + "/v1/metadata"
sql_url = os.getenv("HASURA_URL") + "/v2/query"

db_con_response = requests.post(db_con_url, json=db_connection(os.getenv("AQUA_DB")), headers=headers)
sql_response = requests.post(sql_url, json=sql_query(), headers=headers)
