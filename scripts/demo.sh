#!/usr/bin/env bash
# Live demo of the real-time path and the heat-up alert: the simulator runs 30 times faster with more overruns, and
# none of it reaches the history (decision 36). The order is the point: the Lakehouse destination is paused, and
# confirmed paused, before the first demo event is sent.
#
#   scripts/demo.sh start    # capacity on, lh-bronze paused, eh-raw running, simulator + gateway in demo mode
#   scripts/demo.sh stop     # containers down, unsent demo events dropped, lh-bronze running again from "Now"
#   scripts/demo.sh status
#
# Demo settings: DEMO_SCALE (default 0.0333), DEMO_OVERRUN (0.5; 1.0 makes every heat-up an overrun), DEMO_IDLE_H
# (0.5,2). They are passed to Docker Compose as environment variables, so .env is never edited.
# The Activator rule "Heat-up overrun" in act_am_energy is started and stopped by hand: its definition is kept out of
# Git and holds an e-mail address. Needs `az login` (see README) and Docker.
set -euo pipefail
cd "$(dirname "$0")/.."

WS=${WS:-1a1fb8c0-f461-4025-ab89-a04bcfe30756}
ES=${ES:-08f5931f-07af-42fe-b5f7-48b3729001d0}
LH_DEST=${LH_DEST:-69700d79-d8d6-4027-87dd-84c6a75379e2} # lh-bronze
EH_DEST=${EH_DEST:-5ce9a1c4-19fd-49ae-a116-6eccd5b5ebf7} # eh-raw
EVENTSTREAM="https://api.fabric.microsoft.com/v1/workspaces/$WS/eventstreams/$ES"

fabric() { az rest --resource https://api.fabric.microsoft.com "$@"; }

retry() { # retry <command...>: the Eventstream API can refuse calls for a minute after the capacity resumes
  for attempt in 1 2 3 4 5; do
    "$@" && return 0
    echo "  retrying in 15 s ($attempt/5)" >&2
    sleep 15
  done
  return 1
}

dest_status() { fabric --method get --url "$EVENTSTREAM/topology" --query "destinations[?id=='$1'].status | [0]" -o tsv; }

wait_for() { # wait_for <destination id> <status>
  for _ in $(seq 1 36); do
    [ "$(dest_status "$1")" = "$2" ] && return 0
    sleep 5
  done
  echo "destination $1 did not reach $2" >&2
  return 1
}

pause_dest() {
  [ "$(retry dest_status "$1")" = "Paused" ] && return 0
  retry fabric --method post --url "$EVENTSTREAM/destinations/$1/pause" >/dev/null
  wait_for "$1" Paused
}

resume_now() { # "Now": whatever arrived while the destination was paused is skipped
  [ "$(retry dest_status "$1")" = "Running" ] && return 0
  retry fabric --method post --url "$EVENTSTREAM/destinations/$1/resume" \
    --headers "Content-Type=application/json" --body '{"startType":"Now"}' >/dev/null
  wait_for "$1" Running
}

capacity_on() {
  scripts/capacity.sh status | grep -q '"state": "Active"' && return 0
  scripts/capacity.sh resume >/dev/null
  for _ in $(seq 1 30); do
    scripts/capacity.sh status | grep -q '"state": "Active"' && return 0
    sleep 10
  done
  echo "the capacity did not become Active" >&2
  return 1
}

drop_unsent_events() { # demo events still in the gateway buffer would otherwise be sent to bronze next time
  docker compose run --rm --no-deps -T --entrypoint node gateway -e "
const { DatabaseSync } = require('node:sqlite');
const db = new DatabaseSync('/data/buffer.sqlite');
const table = db.prepare(\"SELECT count(*) AS n FROM sqlite_master WHERE type = 'table' AND name = 'events'\").get().n;
const n = table ? db.prepare('SELECT count(*) AS n FROM events').get().n : 0;
if (n) db.exec('DELETE FROM events');
console.log('gateway buffer: ' + n + ' unsent demo events dropped, sequence numbers kept');
" 2>/dev/null | grep 'gateway buffer'
  docker compose down >/dev/null 2>&1 # removes the network that `run` created
}

case "${1:-status}" in
  start)
    echo "1/4 capacity"; capacity_on
    echo "2/4 lh-bronze paused (before any demo event)"; pause_dest "$LH_DEST"
    echo "3/4 eh-raw running"; resume_now "$EH_DEST"
    echo "4/4 simulator and gateway in demo mode"
    DURATION_SCALE=${DEMO_SCALE:-0.0333} HEATUP_OVERRUN_PROBABILITY=${DEMO_OVERRUN:-0.5} \
      IDLE_BETWEEN_JOBS_H=${DEMO_IDLE_H:-0.5,2} docker compose up -d --build
    echo "Demo running. Now start the Activator rule 'Heat-up overrun' (act_am_energy) and open rtd_am_energy."
    ;;
  stop)
    echo "1/3 containers down"; docker compose down
    echo "2/3 gateway buffer"; drop_unsent_events
    echo "3/3 lh-bronze running from Now"; resume_now "$LH_DEST"
    echo "Demo stopped. Stop the Activator rule 'Heat-up overrun', then: scripts/capacity.sh pause"
    ;;
  status)
    scripts/capacity.sh status | grep '"state"'
    if scripts/capacity.sh status | grep -q '"state": "Active"'; then
      fabric --method get --url "$EVENTSTREAM/topology" --query "destinations[].{destination: name, status: status}" -o table
    fi
    docker compose ps --format '{{.Service}}: {{.State}} ({{.Status}})'
    ;;
  *)
    echo "usage: scripts/demo.sh start|stop|status" >&2
    exit 2
    ;;
esac
