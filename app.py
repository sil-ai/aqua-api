from fastapi import FastAPI
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

import json

#TODO
# Initialize a connection to the DGraph DB
# Use environment variables

# Creates the FastAPI app object
def create_app():

    app = FastAPI()

    @app.get("/")
    def read_root():
        return {"Hello": "World"}

    @app.get("/version")
    def list_version():
        # Connect to DGraph
        dgraph_url = 'https://blue-surf-460035.us-east-1.aws.cloud.dgraph.io/graphql'

        transport = RequestsHTTPTransport(
                url=dgraph_url, verify=True, retries=3
                )

        client = Client(transport=transport, fetch_schema_from_transport=True)

        query = gql(
            (
                """
                    query MyQuery { 
                        queryBibleVersion {
                            id
                            name
                            abbreviation
                            isoLanguage {
                                iso639
                            }
                            isoScript {
                                iso15924
                            }
                            rights
                        }
                    }
                """
                )
            )
        result = client.execute(query)
        version_data = []
        for i in range(len(result['queryBibleVersion'])):
            ids = result['queryBibleVersion'][i]['id']
            name = result['queryBibleVersion'][i]['name']
            abbv = result['queryBibleVersion'][i]['abbreviation']
            iso = result['queryBibleVersion'][i]['isoLanguage']['iso639']
            script = result['queryBibleVersion'][i]['isoScript']['iso15924']
            rights = result['queryBibleVersion'][i]['rights']
            
            ind_data = {
                    'id': ids, 'name': name, 'abbreviation': abbv,
                    'language': iso, 'script': script, 'rights': rights
                    }

            version_data.append(ind_data)

        return {'data': version_data}

    return app

# create app
app = create_app()
