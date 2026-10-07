#!/bin/sh
# Custom entrypoint for postgres-b (Phase 5).
#
# First boot (empty volume): pg_basebackup from postgres-a, then start normally.
# Subsequent boots: skip basebackup, start as streaming replica as usual.
#
# This avoids a separate init container so Docker Compose never accidentally
# re-triggers a dependency restart that could promote the standby.
set -e

DATA=/var/lib/postgresql/data

if [ ! -f "$DATA/PG_VERSION" ]; then
    echo "postgres-b: first boot – running pg_basebackup from postgres-a ..."
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
    echo "postgres-b: pg_basebackup done – standby.signal and primary_conninfo written"
else
    echo "postgres-b: data directory already initialised, starting as replica"
fi

# Hand off to the official postgres entrypoint which handles permissions,
# su-exec to the postgres user, and final startup.
exec docker-entrypoint.sh postgres
