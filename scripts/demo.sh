#!/usr/bin/env bash
# One-command "kill a broker, zero loss" demo: brings up the 3-broker Compose cluster,
# runs :chaos:demo (load + a mid-publish SIGKILL of broker-2 + loss/duplication/gap
# verification), tears the cluster back down, and exits with the run's verdict code.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker/docker-compose.yml"

MESSAGES=20000
KILL_AT=""
KEEP_UP=false
NO_BUILD=false

usage() {
  cat <<'EOF'
Usage: scripts/demo.sh [options]

Brings up the 3-broker Compose cluster, runs the one-command failover demo
(load + a mid-publish SIGKILL of broker-2 + verification), and tears the
cluster back down.

Options:
  --messages N     Number of messages to publish (default: 20000)
  --kill-at N       Ack count at which broker-2 is killed (default: messages / 10)
  --keep-up         Skip teardown after the run (cluster stays up)
  --no-build        Skip --build on `docker compose up`
  -h, --help        Show this help and exit
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --messages)
      MESSAGES="$2"
      shift 2
      ;;
    --kill-at)
      KILL_AT="$2"
      shift 2
      ;;
    --keep-up)
      KEEP_UP=true
      shift
      ;;
    --no-build)
      NO_BUILD=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if ! docker info >/dev/null 2>&1; then
  echo "error: docker is not reachable — is Docker running?" >&2
  exit 2
fi
if ! docker compose version >/dev/null 2>&1; then
  echo "error: 'docker compose' is not available" >&2
  exit 2
fi

cleanup() {
  # Restart broker-2 in case the runner died before its own restart step, so a re-run
  # (or --keep-up inspection) starts from a healthy cluster.
  docker compose -f "$COMPOSE_FILE" start broker-2 >/dev/null 2>&1 || true
  if [[ "$KEEP_UP" != true ]]; then
    docker compose -f "$COMPOSE_FILE" down -v
  fi
}
trap cleanup EXIT

UP_ARGS=(-f "$COMPOSE_FILE" up -d --wait)
if [[ "$NO_BUILD" != true ]]; then
  UP_ARGS+=(--build)
fi
echo ">>> bringing up the 3-broker cluster"
docker compose "${UP_ARGS[@]}"

echo ">>> waiting for all brokers to join the cluster"
DEADLINE=$((SECONDS + 90))
for broker in broker-1 broker-2 broker-3; do
  while true; do
    if docker compose -f "$COMPOSE_FILE" logs "$broker" 2>/dev/null | grep -q "joined cluster"; then
      break
    fi
    if (( SECONDS >= DEADLINE )); then
      echo "error: $broker did not log 'joined cluster' within 90s" >&2
      exit 1
    fi
    sleep 1
  done
done

GRADLE_ARGS=(-q ":chaos:demo" "-Pmessages=$MESSAGES")
if [[ -n "$KILL_AT" ]]; then
  GRADLE_ARGS+=("-PkillAt=$KILL_AT")
fi

echo ">>> running the demo ($MESSAGES messages, broker-2 killed mid-publish)"
RUNNER_EXIT=0
if ! (cd "$ROOT_DIR" && ./gradlew "${GRADLE_ARGS[@]}"); then
  RUNNER_EXIT=$?
  if [[ -z "${JAVA_HOME:-}" ]]; then
    echo "hint: if Gradle failed to find a JDK 21 toolchain, set JAVA_HOME to a JDK 21" \
      "install (this machine's system JDK alone is not enough)." >&2
  fi
fi

if [[ "$RUNNER_EXIT" -eq 0 ]]; then
  echo ">>> DEMO PASSED"
else
  echo ">>> DEMO FAILED (exit $RUNNER_EXIT)"
fi
exit "$RUNNER_EXIT"
