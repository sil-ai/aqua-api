# AQuA API

# Local development

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
