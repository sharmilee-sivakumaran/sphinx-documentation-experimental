This is a script that puts a rabbitmq dump into your local PostgresDB as json.

This lets us do qc and queries on our end when we have the dump.
We don't need to wait for pillar/kraken ingestion piece to be done before querying.

- Requires the PostgresDB set up with user credentials and read/write access before running

Example:

`python insert.py indonesia.q -t indonesia`

Inserts the rabbitmq file `indonesia.q` into an `indonesia` table in the DB defined in `settings.yaml`
