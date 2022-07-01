import json
import os
from datetime import date

from fastapi import FastAPI, Body, Security, Depends, HTTPException, status
from fastapi import File, UploadFile
from fastapi.security.api_key import APIKeyQuery, APIKeyCookie, APIKeyHeader, APIKey
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.security import OAuth2PasswordBearer
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from starlette.status import HTTP_403_FORBIDDEN
from starlette.responses import RedirectResponse, JSONResponse
import pandas as pd
import numpy as np

import queries
import bible_loading
from key_fetch import get_secret

# Get valid API keys
api_keys = get_secret(
            os.getenv("AWS_ACCESS_KEY"),
            os.getenv("AWS_SECRET_KEY")
            )

my_col = ['book', 'chapter', 'verse']
vref = pd.read_csv('vref.txt', sep=' |:', names=my_col, engine='python')

# Use Token authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


def api_key_auth(api_key: str = Depends(oauth2_scheme)):
    if api_key not in api_keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Forbidden"
        )


# Creates the FastAPI app object
def create_app():
    app = FastAPI()

    # Configure connection to the GRAPHQL endpoint
    headers = {'x-hasura-admin-secret': os.getenv("GRAPHQL_SECRET")}
    transport = RequestsHTTPTransport(
            url=os.getenv("GRAPHQL_URL"), verify=True, retries=3, headers=headers
            )


    @app.get("/")
    async def read_root():
        return {"Hello": "World"}


    @app.get("/version", dependencies=[Depends(api_key_auth)])
    async def list_version():
        list_versions = queries.list_versions_query()

        with Client(transport=transport, fetch_schema_from_transport=True) as client:

            query = gql(list_versions)

            result = client.execute(query)
            version_data = []

            for version in result["bibleVersion"]: 
                ind_data = {
                        "id": version["id"], 
                        "name": version["name"], 
                        "abbreviation": version["abbreviation"],
                        "language": version["language"]["iso639"], 
                        "script": version["script"]["iso15924"], 
                        "rights": version["rights"]
                        }

                version_data.append(ind_data)

        return {"data": version_data}

    
    @app.post("/upload_bible", dependencies=[Depends(api_key_auth)])
    async def upload_bible(files: List[UploadFile] = File(...)):
        revision = queries.bible_revision(str(date.today()))

        for file in files:
            try:
                contents = await file.read()
                verses = []
                
                with open(file.filename, "r") as f:
                    for line in f:
                        if line == "\n" or line == "" or line == " ":
                            verses.append(np.nan)
                        else:
                            verses.append(line.replace("\n", ""))

            except Exception:
                return {"message": "There was an error uploading the file(s)"}
            
            finally:                
                with Client(transport=transport,
                        fetch_schema_from_transport=True) as client:

                    query = gcl(revision)

                    revision = client.execute(query)
                    revision_id = revision["bibleRevision"]["id"]

                    bibleRevision = []
                    for verse in verses:
                        bibleRevision.append(revision_id)

                    bible_loading.upload_bible("vref.txt", verses, bibleRevision)

                await file.close()

        return {
                "message": f"Successfuly uploaded {[file.filename for file in files]}", 
                "Revision ID": revision_id
                }


    return app


# create app
app = create_app()
