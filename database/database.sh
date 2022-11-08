#!/bin/bash

export NEW_HASURA_URL=$1 NEW_HASURA_SECRET=$2 NEW_DB=$3 NEW_DB_NAME=$4

python graphql_setup.py

python migration_data_loading/load_iso_codes.py
python migration_data_loading/load_bible_locations.py
python migration_data_loading/load_questions_answers.py
