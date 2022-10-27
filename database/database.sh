#!/bin/bash

export NEW_HASURA_URL=$1 NEW_HASURA_SECRET=$2 NEW_DB=$3 

python graphql_setup.py
python load_iso_codes.py
python load_bible_locations.py
python load_questions_answers.py
