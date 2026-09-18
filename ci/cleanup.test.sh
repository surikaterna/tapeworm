#!/usr/bin/env bash
# Tests the real host orchestrator; only the injected failure uses a Docker wrapper.
set -Eeuo pipefail
ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"
read -r ID < /proc/sys/kernel/random/uuid
SCRATCH=$(mktemp -d "${TMPDIR:-/tmp}/tapeworm-ci-test-XXXXXXXX")
export REAL_DOCKER
REAL_DOCKER=$(command -v docker)
OTHER="tw-test-$ID"
FAIL="fail-$ID"
SIGNAL="signal-$ID"
PROJECT=org.tapeworm.ci.project=qualification
PID=''

cleanup() {
    [[ -z $PID ]] || kill -TERM "$PID" 2>/dev/null || :
    RUN_ID="$FAIL" ./ci/qualify.sh cleanup
    RUN_ID="$SIGNAL" ./ci/qualify.sh cleanup
    docker rm -fv "$OTHER"
    docker network rm "$OTHER"
    docker volume rm "$OTHER"
    rm -rf -- "$SCRATCH"
}
trap cleanup EXIT
docker network create --label "$PROJECT" --label "org.tapeworm.ci.run=$OTHER" "$OTHER"
docker volume create --label "$PROJECT" --label "org.tapeworm.ci.run=$OTHER" "$OTHER"
docker run -d --name "$OTHER" --network "$OTHER" --label "$PROJECT" \
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
PATH="$SCRATCH:$PATH" RUN_ID="$FAIL" ./ci/qualify.sh || status=$?
[[ $status == 42 ]]
[[ -z $(docker ps -aq --filter "label=org.tapeworm.ci.run=$FAIL") ]]
[[ -z $(docker network ls -q --filter "label=org.tapeworm.ci.run=$FAIL") ]]
[[ -z $(docker volume ls -q --filter "label=org.tapeworm.ci.run=$FAIL") ]]
printf 'Forced Docker-start failure preserved exit 42 and cleaned owned resources\n'

RUN_ID="$SIGNAL" ./ci/qualify.sh & PID=$!
end=$((SECONDS + 90))
until [[ -n $(docker ps -q --filter "name=tw-$SIGNAL-mongo") ]]; do
    (( SECONDS < end )) || exit 1
    sleep 1
done
kill -TERM "$PID"
status=0
wait "$PID" || status=$?
PID=''
[[ $status == 143 ]]
[[ -z $(docker ps -aq --filter "label=org.tapeworm.ci.run=$SIGNAL") ]]
[[ -z $(docker network ls -q --filter "label=org.tapeworm.ci.run=$SIGNAL") ]]
[[ -z $(docker volume ls -q --filter "label=org.tapeworm.ci.run=$SIGNAL") ]]
[[ $(docker inspect --format '{{.State.Running}}' "$OTHER") == true ]]
docker network inspect "$OTHER" --format '{{.Id}}'
docker volume inspect "$OTHER" --format '{{.Name}}'
RUN_ID="$SIGNAL" ./ci/qualify.sh cleanup
printf 'SIGTERM exit 143, idempotent cleanup, unrelated container/network/volume preserved\n'
