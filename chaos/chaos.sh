#!/usr/bin/env bash
# Failure injection for the multi-region lab.   Usage: chaos/chaos.sh <action> [a|b]
set -euo pipefail
cd "$(dirname "$0")/.."

action="${1:-help}"
r="${2:-}"

dc() { docker compose "$@"; }
other() { [ "$1" = "a" ] && echo b || echo a; }

need_region() {
  [[ "$r" == "a" || "$r" == "b" ]] || { echo "this action needs a region: a or b" >&2; exit 1; }
}

wait_healthy() { # container name
  printf "waiting for %s " "$1"
  for _ in $(seq 1 60); do
    s=$(docker inspect -f '{{.State.Health.Status}}' "$1" 2>/dev/null || echo none)
    [ "$s" = "healthy" ] && { echo "ok"; return; }
    printf "."; sleep 1
  done
  echo "(timeout)"
}

case "$action" in
  region-down)   # total loss of the region: service + database + kafka
    need_region
    dc stop "service-$r" "postgres-$r" "redpanda-$r"
    echo ">> region $r is DOWN. Router should move everything to $(other "$r") within ~2-3s." ;;

  region-up)     # failback: bring the region back (data is kept, unless you ran region-wipe)
    need_region
    dc up -d "postgres-$r" "redpanda-$r" "redpanda-init-$r" "service-$r"
    wait_healthy "mr-postgres-$r"; wait_healthy "mr-redpanda-$r"
    echo ">> region $r is coming back. Watch: router rise, outbox drain, replication lag shrinking." ;;

  region-wipe)   # DISASTER: the region is gone and its disks too
    need_region
    dc rm -sf "service-$r" "postgres-$r" "redpanda-$r" "redpanda-init-$r"
    docker volume rm -f "mr-postgres-$r-data" "mr-redpanda-$r-data" >/dev/null
    echo ">> region $r destroyed WITH its data. Next: region-up $r, then backfill $r (see ROADMAP phase 4)." ;;

  backfill)      # rebuild an empty region's customers table from the other region's database
    need_region
    o="$(other "$r")"
    echo ">> copying customers from region $o into region $r (existing ids are kept)"
    docker exec "mr-postgres-$o" pg_dump -U app -d customers --data-only --inserts --on-conflict-do-nothing -t customers \
      | docker exec -i "mr-postgres-$r" psql -q -U app -d customers
    echo ">> done" ;;

  reset-offsets) # make region <r> re-read the whole topic of the other region (group lives in the OTHER cluster)
    need_region
    o="$(other "$r")"
    dc stop "service-$r"
    docker exec "mr-redpanda-$o" rpk group seek "replicator-$r" --to start
    dc start "service-$r"
    echo ">> region $r will replay every event of $o.customers (idempotent: stale/duplicate events are skipped)" ;;

  db-down)   need_region; dc stop "postgres-$r";  echo ">> postgres-$r stopped: /health of region $r returns 503, router drains it." ;;
  db-up)     need_region; dc start "postgres-$r"; wait_healthy "mr-postgres-$r" ;;

  kafka-down) need_region; dc stop "redpanda-$r"
    echo ">> redpanda-$r stopped: writes keep working in $r, rows go PUBLISH_FAILED (outbox backlog grows)." ;;
  kafka-up)   need_region; dc start "redpanda-$r"; wait_healthy "mr-redpanda-$r"
    echo ">> redpanda-$r back: the relay republishes PUBLISH_FAILED rows." ;;

  partition)     # region r loses the WAN: clients still reach it (edge network), but it can't talk to the other region
    need_region
    docker network disconnect mr-wan "mr-service-$r"
    docker network disconnect mr-wan "mr-redpanda-$r"
    echo ">> region $r is PARTITIONED from the WAN. Both regions keep accepting writes (split brain risk)." ;;
  heal)
    need_region
    docker network connect --alias "service-$r"  mr-wan "mr-service-$r"
    docker network connect --alias "redpanda-$r" mr-wan "mr-redpanda-$r"
    echo ">> WAN restored for region $r. Replication catches up from the committed offsets." ;;

  status)
    dc ps --format 'table {{.Name}}\t{{.Status}}'
    echo; echo "router view (be_active_active / be_failover):"
    curl -s 'http://localhost:8404/stats;csv' | awk -F, '$1 ~ /^be_/ && $2 ~ /^region/ {printf "  %-18s %-10s %s\n", $1, $2, $18}' ;;

  *)
    cat <<'USAGE'
actions:
  region-down  <a|b>   stop service+postgres+redpanda of a region (total loss)
  region-up    <a|b>   start them again (failback)
  region-wipe  <a|b>   destroy the region AND its volumes (disaster with data loss)
  backfill     <a|b>   copy customers from the other region's database into this one
  reset-offsets <a|b>  make this region replay the other region's whole topic
  db-down|db-up       <a|b>
  kafka-down|kafka-up <a|b>
  partition|heal      <a|b>   cut / restore the region's WAN link
  status               containers + what the router thinks of each region
USAGE
    ;;
esac
