#!/usr/bin/env bash
set -Eeuo pipefail
set +x
: "${ROOT:?}" "${ART:?}" "${RUN_ID:?}"
: "${DOCKER_BIN:=docker}"
DOCKER_EXEC=$(command -v -- "$DOCKER_BIN" 2>/dev/null) || DOCKER_EXEC=''
if [[ ! -f $DOCKER_EXEC || ! -x $DOCKER_EXEC ]]; then
    printf 'Docker CLI unavailable: DOCKER_BIN=%s. Set DOCKER_BIN to an executable Docker CLI path or command.\n' "$DOCKER_BIN" >&2
    exit 127
fi
export DOCKER_BIN
cd "$ROOT"
RUNNER=$("$DOCKER_BIN" image inspect --format '{{.Id}}' "tapeworm-ci:$RUN_ID")
IMAGE=$(< "$ART/image-id")
[[ $IMAGE =~ ^sha256:[a-f0-9]{64}$ ]]
COMMON=$(git rev-parse --path-format=absolute --git-common-dir)
MOUNTS=()
[[ $COMMON == "$ROOT"/* ]] || MOUNTS+=(--mount "type=bind,src=$COMMON,dst=$COMMON,readonly")

node_runner() {
    "$DOCKER_BIN" run --rm --user "$(id -u):$(id -g)" --label "$LABEL" --label "$PROJECT" \
        --mount "type=bind,src=$ROOT,dst=$ROOT" --workdir "$ROOT" "${MOUNTS[@]}" \
        -e HOME="$ART/home" -e GIT_OPTIONAL_LOCKS=0 -e BRANCH_NAME -e TAG_NAME "$RUNNER" "$@"
}

preflight() {
    [[ $(< "$ART/qualified-image-id") == "$IMAGE" ]]
    node_runner node ci/policy.mjs release-preflight "$ART"
    [[ $("$DOCKER_BIN" image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$IMAGE") == "$(< "$ART/revision")" ]]
}

publish() {
    : "${NPM_TOKEN:?}" "${DOCKER_CREDS_USR:?}" "${DOCKER_CREDS_PSW:?}" "${DOCKER_REGISTRY:?}"
    local host version revision prefix
    prefix=$DOCKER_REGISTRY
    host=$(node_runner node --input-type=module -e 'import {registry} from "./ci/policy.mjs"; console.log(registry(process.argv[1]).host)' "$prefix")
    version=$(node_runner node -p 'require("./packages/tapeworm_dispatcher_mdb_rmq/package.json").version')
    revision=$(< "$ART/revision")
    SECRET_DIR=$(mktemp -d /tmp/tapeworm-release-XXXXXXXX)
    trap 'rm -rf -- "$SECRET_DIR"' EXIT
    mkdir "$SECRET_DIR/docker"
    printf '%s\n' '//registry.npmjs.org/:_authToken=${NPM_TOKEN}' > "$SECRET_DIR/npmrc"
    "$DOCKER_BIN" run --rm --user "$(id -u):$(id -g)" --label "$LABEL" --label "$PROJECT" \
        --mount "type=bind,src=$ROOT,dst=$ROOT" --workdir "$ROOT" "${MOUNTS[@]}" \
        --mount "type=bind,src=$SECRET_DIR/npmrc,dst=/tmp/release.npmrc,readonly" \
        -e HOME="$ART/home" -e GIT_OPTIONAL_LOCKS=0 -e NPM_TOKEN \
        -e NPM_CONFIG_USERCONFIG=/tmp/release.npmrc "$RUNNER" npm run changeset:publish
    printf '%s' "$DOCKER_CREDS_PSW" | "$DOCKER_BIN" --config "$SECRET_DIR/docker" login -u "$DOCKER_CREDS_USR" --password-stdin "$host"
    "$DOCKER_BIN" tag "$IMAGE" "$prefix/tapeworm-dispatcher:$revision-$RUN_ID"
    "$DOCKER_BIN" --config "$SECRET_DIR/docker" push "$prefix/tapeworm-dispatcher:$revision-$RUN_ID"
    "$DOCKER_BIN" tag "$IMAGE" "$prefix/tapeworm-dispatcher:$version"
    "$DOCKER_BIN" --config "$SECRET_DIR/docker" push "$prefix/tapeworm-dispatcher:$version"
    "$DOCKER_BIN" tag "$IMAGE" "$prefix/tapeworm-dispatcher:latest"
    "$DOCKER_BIN" --config "$SECRET_DIR/docker" push "$prefix/tapeworm-dispatcher:latest"
}

preflight
case ${1:?} in release-preflight) ;; publish) publish ;; *) exit 2 ;; esac
