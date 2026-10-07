#!/bin/sh
# Runs in a one-shot init container before postgres-b starts.
# Populates the postgres-b data volume from postgres-a via pg_basebackup.
# If the data directory already has PG_VERSION (e.g. after a restart without clean),
# it skips the backup so the existing standby is not wiped.
set -e

DATA=/var/lib/postgresql/data

if [ -f "$DATA/PG_VERSION" ]; then
    echo "postgres-b: data directory already initialised, skipping pg_basebackup"
    exit 0
fi

echo "postgres-b: running pg_basebackup from postgres-a ..."
PGPASSWORD=replicator pg_basebackup \
    -h postgres-a \
    -p 5432 \
    -U replicator \
    -D "$DATA" \
    -Fp \
    -Xs \
    -R \
    -P \
    --checkpoint=fast

echo "postgres-b: pg_basebackup done – standby.signal and primary_conninfo written by -R flag"
