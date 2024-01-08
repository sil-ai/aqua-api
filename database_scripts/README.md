* Go into the Neon dashboard and select `manage` in the branches blob, then select `New Branch`.
* Make sure the `Parent Branch` is the branch that is empty except for seed data, and that a new `compute endpoint` is being created.
* The new endpoint will need to be subbed in for the old endpoint in the current database connection string.
* Create a new Hasura project and go into the `Data` tab. 
* Name the database "default" and enter the database connection string. 
* After connecting the database and project successfully the new `tables` and foreign-key relationships` will need to be tracked.
