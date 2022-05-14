A barebones, obfuscated version of a pipeline.
Workflow is MongoDB -> S3 -> PostgreSQL.

Data is pushed into tables in Postgres where they are further transformed before being copied into tables used for production.
Further transformations include unnesting JSON arrays, validating dates, extracting substrings from categorical names.

Daily transactions contains information on all new records coming in daily. Certain columns come in nested JSON arrays, which have to be turned into a format accepted by CSV and Postgres COPY workflow.

Daily forecasts contains forecasts ouput by a machine learning model, which are also stored in nested arrays.

