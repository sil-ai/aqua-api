#!/bin/bash

export HASURA_URL=$1 HASURA_SECRET=$2 AQUA_DB=$3

python graphql_setup.py
python load_iso_codes.py
python load_bible_locations.py
python load_questions_answers.py
