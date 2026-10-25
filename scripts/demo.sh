#!/usr/bin/env bash
# make demo: a fresh stack, a seeded v1 producer run through every layer and the end-to-end check
# through Trino; then schema v2 (the registry rejects a breaking schema), a second run, the check
# over both manifests, and the check again after maintenance (docs/PLAN.md "make demo").
set -Eeuo pipefail # -E: the ERR trap also fires inside functions

export MSYS_NO_PATHCONV=1 # Git Bash on Windows would rewrite /runs/... into a Windows path

RUN1=/runs/run1.json
RUN2=/runs/run2.json
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
run "produce --seed 42 (schema v1)" kafka run --rm producer produce --seed 42 --manifest "${RUN1}"
run "lag --wait" consumer run --rm cli lag --wait --timeout 300
run "silver (available-now)" iceberg-rest \
  run --rm --build spark-job silver --trigger available-now
run "gold" iceberg-rest run --rm spark-job gold
run "check run 1" trino run --rm cli check --manifest "${RUN1}"

run "register-schemas --version 2" schema-registry run --rm cli register-schemas --version 2
step="register-schemas --check-only v2-breaking (must be rejected)" logs_of=schema-registry
echo
echo "==> ${step}"
# Only the CLI's own rejection counts: a registry error, a bad option or a compose failure must
# fail the demo too, not pass as "rejected".
if out=$(docker compose run --rm cli register-schemas --check-only shop_event.v2-breaking.avsc 2>&1)
then
  echo "${out}"
  echo "the registry accepted a breaking schema" >&2
  false
elif [[ ${out} != *"incompatible:"* ]]; then
  echo "${out}" >&2
  echo "the check failed without the registry's rejection" >&2
  false
fi
echo "${out}"
run "produce --seed 43 (schema v2)" kafka run --rm producer produce --schema-version 2 \
  --base-time 2026-01-02T00:00:00Z --seed 43 --manifest "${RUN2}"
run "lag --wait (run 2)" consumer run --rm cli lag --wait --timeout 300
run "silver (run 2)" iceberg-rest run --rm spark-job silver --trigger available-now
run "gold (run 2)" iceberg-rest run --rm spark-job gold
run "check runs 1 and 2" trino run --rm cli check --manifest "${RUN1}" --manifest "${RUN2}"
run "dlq (first 3)" kafka run --rm cli dlq --limit 3

run "maintain" iceberg-rest run --rm spark-job maintain
run "check after maintain" trino run --rm cli check --manifest "${RUN1}" --manifest "${RUN2}"

echo
echo "demo passed. Query it: docker compose exec trino trino --catalog lake"
echo "Clean up with: make down"
