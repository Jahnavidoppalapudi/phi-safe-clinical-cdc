#!/bin/sh
# Least-privilege login for Debezium: may stream changes and read the published tables, nothing else.
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v cdc_password="$CDC_PASSWORD" -v db="$POSTGRES_DB" <<'SQL'
create role cdc_reader with login replication password :'cdc_password';
grant connect on database :"db" to cdc_reader;
grant usage on schema public to cdc_reader;
grant select on patients, encounters, lab_results, consents to cdc_reader;
SQL
