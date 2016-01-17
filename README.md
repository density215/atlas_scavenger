RIPE Atlas scavenger
====================

Copies measurements from the RIPE Atlas API to a postgres (>=9.4) database and put it in a table called `meas` in a jsonb field called `body`.

The `id` field is a normal serial marked field, hence there is no coordination right now between the id in the jsonb field and
the primary key.

Usage of ./atlas_scavenger:

    -dbname string
    	specify name of the database where the measurements will be stored. (default "atlas-msm")
    -host string
    	specify hostname of the server where the database is running (default "127.0.0.1")
    -number_of_runs uint
    	specify the maximum number of separate rest queries (500 measurements per query). (default 4000)
    -password string
    	specify password to connect to database (required)
    -resume
    	resume from the last measurement in database.
    -start_id uint
    	specify starting measurement id from atlas API. (default 1000000)
    -username string
    	specify username to connect to database (required)
    	
Not specifying `--resume` will destroy the `meas` table and create it afresh. 