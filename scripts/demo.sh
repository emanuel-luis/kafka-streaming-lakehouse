#!/usr/bin/env bash
# make demo: a fresh stack, one seeded producer run through every layer, then the end-to-end check
# through Trino before and after maintenance (docs/PLAN.md "make demo").
set -Eeuo pipefail # -E: the ERR trap also fires inside functions

export MSYS_NO_PATHCONV=1 # Git Bash on Windows would rewrite /runs/... into a Windows path

MANIFEST=/runs/run1.json
step="" logs_of=""

on_error() {
  local code=$?
  echo >&2
  echo "demo failed at step: ${step} (exit ${code})" >&2
  docker compose --profile tools ps -a >&2 || true
  # A `run --rm` container is gone and its output is already above, so show the logs of the
  # service it talks to, or of every service when the stack itself did not come up.
  echo "--- recent logs of ${logs_of:-every service} ---" >&2
  # shellcheck disable=SC2086 # empty means every service
  docker compose logs --tail 60 ${logs_of} >&2 || true
  exit "${code}"
}
trap on_error ERR

run() { # run <step> <service whose logs help on failure, or ""> <compose args...>
  step=$1 logs_of=$2
  shift 2
  echo
  echo "==> ${step}"
  docker compose "$@"
}

run "fresh stack (down -v)" "" --profile tools --profile ui down -v --remove-orphans
run "up --wait" "" up -d --build --wait
run "produce --seed 42" kafka run --rm producer produce --seed 42 --manifest "${MANIFEST}"
run "lag --wait" consumer run --rm cli lag --wait --timeout 300
run "silver (available-now)" iceberg-rest \
  run --rm --build spark-job silver --trigger available-now
run "gold" iceberg-rest run --rm spark-job gold
run "check" trino run --rm cli check --manifest "${MANIFEST}"
run "maintain" iceberg-rest run --rm spark-job maintain
run "check after maintain" trino run --rm cli check --manifest "${MANIFEST}"

echo
echo "demo passed. Query it: docker compose exec trino trino --catalog lake"
echo "Clean up with: make down"
