#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"
if [[ -z ${RUN_ID:-} ]]; then read -r RUN_ID < /proc/sys/kernel/random/uuid; fi
[[ $RUN_ID =~ ^[a-zA-Z0-9][a-zA-Z0-9-]{0,70}$ ]] || { printf 'Invalid RUN_ID\n' >&2; exit 2; }
export RUN_ID
ART="$ROOT/.ci-artifacts/$RUN_ID"
NET="tw-$RUN_ID"
LABEL="org.tapeworm.ci.run=$RUN_ID"
PROJECT="org.tapeworm.ci.project=qualification"
mkdir -p "$ART/home" "$ART/tmp"
export ROOT ART NET LABEL PROJECT

cleanup() {
    local result=0 ids
    ids=$(docker ps -aq --filter "label=$LABEL" --filter "label=$PROJECT") || return 1
    if [[ -n $ids ]]; then docker rm -fv $ids || result=1; fi
    ids=$(docker network ls -q --filter "label=$LABEL" --filter "label=$PROJECT") || return 1
    if [[ -n $ids ]]; then docker network rm $ids || result=1; fi
    ids=$(docker volume ls -q --filter "label=$LABEL" --filter "label=$PROJECT") || return 1
    if [[ -n $ids ]]; then docker volume rm $ids || result=1; fi
    return "$result"
}

diagnostics() {
    local id
    for id in $(docker ps -aq --filter "label=$LABEL" --filter "label=$PROJECT"); do
        docker logs --tail 200 "$id" > "$ART/$id.log" 2>&1 || :
        docker inspect --format '{{json .State}}' "$id" > "$ART/$id.state.json" || :
    done
}

finish() {
    local status=$?
    trap - EXIT INT TERM
    if (( status != 0 )); then diagnostics; fi
    if ! cleanup; then
        printf 'Cleanup incomplete; recover by run labels: %s\n' "$RUN_ID" >&2
        if (( status == 0 )); then status=1; fi
    fi
    if [[ -n ${SENTINEL:-} ]]; then rm -f -- "$SENTINEL"; fi
    if (( status == 0 )) && [[ -n ${IMAGE:-} ]]; then
        printf '%s\n' "$IMAGE" > "$ART/qualified-image-id"
        printf 'QUALIFIED run=%s image=%s\n' "$RUN_ID" "$IMAGE"
    fi
    if [[ -n ${LOG_PID:-} ]]; then
        exec 1>&3 2>&4
        if ! wait "$LOG_PID" && (( status == 0 )); then status=1; fi
    fi
    exit "$status"
}

case ${1:-qualify} in
    cleanup) cleanup; exit ;;
    release-preflight|publish) exec "$ROOT/ci/release.sh" "$1" ;;
    qualify) [[ ! -e $ART/qualification.log ]] || { printf 'Use a fresh RUN_ID\n' >&2; exit 2; } ;;
    *) printf 'Usage: ci/qualify.sh [qualify|cleanup|release-preflight|publish]\n' >&2; exit 2 ;;
esac
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
exec 3>&1 4>&2
exec > >(tee >(tail -c 2097152 > "$ART/qualification.log")) 2>&1
LOG_PID=$!

preflight() {
    [[ $(uname -s) == Linux ]]
    command -v docker; command -v git; command -v timeout
    bash -n ci/qualify.sh ci/services.sh ci/release.sh ci/cleanup.test.sh
    [[ $(docker info --format '{{.OSType}}') == linux ]]
    [[ $(< .nvmrc) == 26.9.0 ]]
    [[ -w $ROOT ]]
    printf 'Run %s; source %s; workspace %s\n' "$RUN_ID" "$(git rev-parse HEAD)" "$ROOT"
    docker version
    local image
    for image in node:26.9.0-alpine mongo:8.3.9 rabbitmq:4.3.6; do docker pull "$image"; done
    docker build --pull -f ci/Dockerfile -t "tapeworm-ci:$RUN_ID" .
    RUNNER=$(docker image inspect --format '{{.Id}}' "tapeworm-ci:$RUN_ID")
    export RUNNER
    docker image inspect --format '{{.Id}} {{json .RepoDigests}}' \
        node:26.9.0-bookworm node:26.9.0-alpine mongo:8.3.9 rabbitmq:4.3.6
}

git_mounts() {
    local dir common admin
    common=$(git rev-parse --path-format=absolute --git-common-dir)
    admin=$(git rev-parse --path-format=absolute --git-dir)
    GIT_MOUNTS=()
    for dir in "$common"; do
        [[ $dir == "$ROOT"/* ]] && continue
        GIT_MOUNTS+=(--mount "type=bind,src=$dir,dst=$dir,readonly")
    done
    if [[ $admin != "$common" && $admin != "$common"/* && $admin != "$ROOT"/* ]]; then
        GIT_MOUNTS+=(--mount "type=bind,src=$admin,dst=$admin,readonly")
    fi
}

runner() {
    docker run --rm --name "tw-$RUN_ID-runner" --label "$LABEL" --label "$PROJECT" \
        --network "$NET" --user "$(id -u):$(id -g)" \
        --mount "type=bind,src=$ROOT,dst=$ROOT" --workdir "$ROOT" \
        --mount "type=bind,src=$ART/tmp,dst=/tmp" "${GIT_MOUNTS[@]}" \
        -e HOME="$ART/home" -e npm_config_cache="$ART/home/npm" -e TMPDIR=/tmp \
        -e GIT_OPTIONAL_LOCKS=0 -e CI=true -e TURBO_TELEMETRY_DISABLED=1 \
        -e TEST_MONGODB_URI='mongodb://mongo:27017/?directConnection=true&replicaSet=rs0' \
        -e TEST_EXPIRY_MONGODB_URI='mongodb://expiry:27017/?directConnection=true&replicaSet=rs0' \
        -e TEST_RABBITMQ_URI='amqp://ci:ci-test-only@rabbit:5672' "$RUNNER" "$@"
}

gates() {
    runner bash -euc '
        test "$(node --version)" = v26.9.0
        test "$(npm --version)" = 11.12.1
        node --input-type=module -e '\''import p from "./package.json" with {type:"json"}; if(p.packageManager!=="npm@11.12.1") process.exit(1)'\''
        printf "\nGATE npm ci\n"; npm ci
        printf "\nGATE pristine owned generated outputs\n"; node ci/artifacts.mjs clean
        printf "\nGATE forced root build\n"; npm run build -- --force --concurrency=1
        printf "\nGATE root check\n"; npm run check -- --force --concurrency=1
        printf "\nGATE CI helper check/test\n"; node_modules/.bin/tsc -p ci/tsconfig.json; node --test ci/*.test.mjs
        printf "\nGATE root test\n"; npm test -- --force --concurrency=1
        printf "\nGATE packed consumer\n"; npm run test:consumer -w tapeworm_dispatcher_mdb_rmq
        printf "\nGATE real services\n"; node ci/image-smoke.mjs services
        printf "\nGATE unfiltered integration\n"; npm run test:integration -w tapeworm_dispatcher_mdb_rmq
    '
}

build_image() {
    printf '\nGATE production build (no cache, generated host outputs present)\n'
    runner node ci/policy.mjs identity "$ART"
    local marker="$ROOT/packages/tapeworm_dispatcher_mdb_rmq/dist/ci-host-sentinel"
    [[ ! -e $marker && ! -L $marker ]]
    SENTINEL=$marker
    printf 'Ignored build-context sentinel\n' > "$SENTINEL"
    local version core_version
    version=$(runner node -p 'require("./packages/tapeworm_dispatcher_mdb_rmq/package.json").version')
    core_version=$(runner node -p 'require("./packages/tapeworm/package.json").version')
    docker build --no-cache --pull --force-rm -f packages/tapeworm_dispatcher_mdb_rmq/Dockerfile \
        --build-arg "REVISION=$(git rev-parse HEAD)" --build-arg "VERSION=$version" \
        --build-arg "CORE_VERSION=$core_version" -t "tapeworm-dispatcher:$RUN_ID" .
    rm -- "$SENTINEL"
    SENTINEL=''
    IMAGE=$(docker image inspect --format '{{.Id}}' "tapeworm-dispatcher:$RUN_ID")
    export IMAGE
    printf '%s\n' "$IMAGE" > "$ART/image-id"
    git rev-parse HEAD > "$ART/revision"
    docker image inspect --format '{{.Id}} {{json .Config.Labels}}' "$IMAGE"
    runner node ci/policy.mjs verify-artifacts "$ART"
    "$ROOT/ci/services.sh" smoke
}

preflight
git_mounts
"$ROOT/ci/services.sh" start
gates
build_image
