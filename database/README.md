For setting up a fresh Hasura instance and database, run the `database.sh` script with the Hasura URL, Hasura Secret Key, Amazon RDS connection string, and name of the new database.
The script uses the 3 arguments given to set environment variables, and then creates the new database and connects it with the new Hasura instance.
Then, it uploads the schema and necessary `iso_codes`, `bible locations`, and `comprehension questions`.
After the script is finished, all tables and foreign keys will need to be tracked by going into the Hasura console for the instance, and selecting `Track All`.
