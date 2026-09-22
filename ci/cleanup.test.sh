#!/usr/bin/env bash
# Tests the real host orchestrator; only the injected failure uses a Docker wrapper.
set -Eeuo pipefail
ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"
: "${DOCKER_BIN:=docker}"
DOCKER_EXEC=$(command -v -- "$DOCKER_BIN" 2>/dev/null) || DOCKER_EXEC=''
if [[ ! -f $DOCKER_EXEC || ! -x $DOCKER_EXEC ]]; then
    printf 'Docker CLI unavailable: DOCKER_BIN=%s. Set DOCKER_BIN to an executable Docker CLI path or command.\n' "$DOCKER_BIN" >&2
    exit 127
fi
export DOCKER_BIN
read -r ID < /proc/sys/kernel/random/uuid
SCRATCH=$(mktemp -d "${TMPDIR:-/tmp}/tapeworm-ci-test-XXXXXXXX")
export REAL_DOCKER
REAL_DOCKER=$(command -v -- "$DOCKER_BIN")
OTHER="tw-test-$ID"
FAIL="fail-$ID"
SIGNAL="signal-$ID"
PROJECT=org.tapeworm.ci.project=qualification
PID=''

cleanup() {
    [[ -z $PID ]] || kill -TERM "$PID" 2>/dev/null || :
    RUN_ID="$FAIL" ./ci/qualify.sh cleanup
    RUN_ID="$SIGNAL" ./ci/qualify.sh cleanup
    "$DOCKER_BIN" rm -fv "$OTHER"
    "$DOCKER_BIN" network rm "$OTHER"
    "$DOCKER_BIN" volume rm "$OTHER"
    rm -rf -- "$SCRATCH"
}
trap cleanup EXIT
"$DOCKER_BIN" network create --label "$PROJECT" --label "org.tapeworm.ci.run=$OTHER" "$OTHER"
"$DOCKER_BIN" volume create --label "$PROJECT" --label "org.tapeworm.ci.run=$OTHER" "$OTHER"
"$DOCKER_BIN" run -d --name "$OTHER" --network "$OTHER" --label "$PROJECT" \
    --label "org.tapeworm.ci.run=$OTHER" --mount "type=volume,src=$OTHER,dst=/sentinel" \
    node:26.9.0-bookworm sleep 300

cat > "$SCRATCH/docker" <<'WRAPPER'
#!/usr/bin/env bash
set -euo pipefail
for arg in "$@"; do
    if [[ $arg == "tw-$RUN_ID-rabbit" && ${1:-} == run ]]; then exit 42; fi
done
exec "$REAL_DOCKER" "$@"
WRAPPER
chmod +x "$SCRATCH/docker"
status=0
DOCKER_BIN="$SCRATCH/docker" RUN_ID="$FAIL" ./ci/qualify.sh || status=$?
[[ $status == 42 ]]
[[ -z $("$DOCKER_BIN" ps -aq --filter "label=org.tapeworm.ci.run=$FAIL") ]]
[[ -z $("$DOCKER_BIN" network ls -q --filter "label=org.tapeworm.ci.run=$FAIL") ]]
[[ -z $("$DOCKER_BIN" volume ls -q --filter "label=org.tapeworm.ci.run=$FAIL") ]]
printf 'Forced Docker-start failure preserved exit 42 and cleaned owned resources\n'

RUN_ID="$SIGNAL" ./ci/qualify.sh & PID=$!
end=$((SECONDS + 90))
until [[ -n $("$DOCKER_BIN" ps -q --filter "name=tw-$SIGNAL-mongo") ]]; do
    (( SECONDS < end )) || exit 1
    sleep 1
done
kill -TERM "$PID"
status=0
wait "$PID" || status=$?
PID=''
[[ $status == 143 ]]
[[ -z $("$DOCKER_BIN" ps -aq --filter "label=org.tapeworm.ci.run=$SIGNAL") ]]
[[ -z $("$DOCKER_BIN" network ls -q --filter "label=org.tapeworm.ci.run=$SIGNAL") ]]
[[ -z $("$DOCKER_BIN" volume ls -q --filter "label=org.tapeworm.ci.run=$SIGNAL") ]]
[[ $("$DOCKER_BIN" inspect --format '{{.State.Running}}' "$OTHER") == true ]]
"$DOCKER_BIN" network inspect "$OTHER" --format '{{.Id}}'
"$DOCKER_BIN" volume inspect "$OTHER" --format '{{.Name}}'
RUN_ID="$SIGNAL" ./ci/qualify.sh cleanup
printf 'SIGTERM exit 143, idempotent cleanup, unrelated container/network/volume preserved\n'
