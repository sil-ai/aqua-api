#!/usr/bin/env bash
# Boot the built Docker image the way the container starts in production and
# probe it, so an image-only failure (a missing COPY, an unimportable module, a
# broken CMD) fails the build instead of shipping silently.
#
# WHY THIS EXISTS (issue #876): CI builds the image (`make build-actions`) and
# runs the test suite against the source TREE (`make test` -> pytest with
# PYTHONPATH=$PWD), but never boots the image itself -- so the container
# filesystem is never exercised by CI. The regression that motivated this:
# schemas/ and api_v4/ were absent from the Dockerfile COPY set, so
# `uvicorn app:app` died at container start with ModuleNotFoundError while every
# CI check stayed green (fixed in 3a65946). Each v4 contract issue (#825-#831)
# adds new top-level modules imported at ASGI load, so this is a recurring class
# of bug, not a one-off.
#
# Dummy AQUA_DB/SECRET_KEY satisfy the fail-fast boot validation (config.py and
# security_routes.utilities reject a missing/empty value) WITHOUT a live
# database: SQLAlchemy engine creation is lazy, so /health (which touches
# nothing external) serves regardless. We probe /health (liveness) and /v4
# (proves the mounted sub-app and the new top-level packages import at ASGI
# load -- the exact regression class above). We deliberately do NOT probe /ready
# (it opens a real DB connection) since this test runs without a database.
#
# The probe runs curl INSIDE the container (docker exec) rather than publishing
# a port to the host (idea from #877, woodwardmw): no host-port mapping means no
# chance of colliding with something already bound on the runner, and no need
# for a SMOKE_PORT escape hatch. This relies on curl being present in the image
# -- it ships in the python:3.11 base, and the Dockerfile's own HEALTHCHECK
# already depends on it.
set -euo pipefail

IMAGE="${REGISTRY:-docker-local}/${IMAGENAME:-aqua-api-dev}:latest"
CONTAINER="aqua-smoke"

cleanup() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

dump_logs_and_fail() {
  echo "ERROR: $1" >&2
  echo "----- docker logs $CONTAINER -----" >&2
  docker logs "$CONTAINER" >&2 || true
  exit 1
}

# curl the app from inside its own container; keeps the probe off the host.
probe() {
  docker exec "$CONTAINER" curl -fsS --connect-timeout 2 --max-time 5 "http://localhost:8000$1"
}

# Clear any leftover container from a previous local run (CI runners are fresh).
cleanup

# Fail fast if the locally built image is missing, and never fall back to a
# remote pull. The whole point is to test the image we JUST built; a stale
# same-tag image in ECR (from a prior deploy) could otherwise be pulled and
# smoke-tested instead, giving a false green. The pre-flight inspect gives a
# clear, actionable message; --pull=never enforces the no-pull guarantee on
# `docker run` even if the tag drifts past the check.
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "ERROR: image '$IMAGE' not found locally -- build it first (make build-actions)." >&2
  echo "       REGISTRY/IMAGENAME must match the build step." >&2
  exit 1
fi

echo "Booting $IMAGE ..."
if ! docker run -d --name "$CONTAINER" --pull=never \
  -e AQUA_DB="postgresql+asyncpg://smoke:smoke@127.0.0.1:5432/smoke" \
  -e SECRET_KEY="smoke-test-secret" \
  "$IMAGE" >/dev/null; then
  echo "ERROR: 'docker run' failed to start the container (see docker output above)." >&2
  exit 1
fi

# Poll liveness until the app is serving (the 8 uvicorn workers need a moment
# to import the app and bind). 30 x 2s = 60s ceiling.
echo "Waiting for /health ..."
for i in $(seq 1 30); do
  if probe /health >/dev/null 2>&1; then
    echo "Container is live."
    break
  fi
  if ! docker ps -q --filter "name=^${CONTAINER}$" | grep -q .; then
    dump_logs_and_fail "container exited before serving /health"
  fi
  if [ "$i" -eq 30 ]; then
    dump_logs_and_fail "container did not serve /health within 60s"
  fi
  sleep 2
done

echo "GET /health"
probe /health || dump_logs_and_fail "/health did not return success"
echo

echo "GET /v4"
probe /v4 || dump_logs_and_fail "/v4 did not return success (v4 sub-app failed to mount?)"
echo

echo "Smoke test passed."
