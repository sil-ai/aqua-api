For setting up a fresh Hasura instance and PostgreSQL, run the `database.sh` script with the Hasura URL, Hasura Secret Key, and the PostgreSQL connection string.
The script uses the 3 arguments given to set environment variables, and then connects Hasura instance with the PostgreSQL database.
Then, it uploads the schema and necessary `iso_codes`, bible locations, and comprehension questions.
After the script is finished, all tables and foreign keys will need to be tracked by going into the Hasura console for the instance, and selecting `Track All`.
