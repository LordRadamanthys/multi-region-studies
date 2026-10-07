#!/bin/bash
# Runs inside the postgres-a container on first boot (docker-entrypoint-initdb.d).
# Creates the replication user and the metrics/exporter user, then opens pg_hba.conf
# to allow streaming replication from the replica.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Streaming replication: postgres-b will connect as this user
    CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicator';

    -- Prometheus postgres_exporter: needs superuser to read pg_control_checkpoint()
    CREATE USER exporter WITH PASSWORD 'exporter' SUPERUSER;
EOSQL

# Allow replica to connect for WAL streaming from any host (lab only; use CIDR in prod)
echo "host replication replicator all md5" >> "$PGDATA/pg_hba.conf"
