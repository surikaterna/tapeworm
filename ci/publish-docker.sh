#!/usr/bin/env bash
set -Eeuo pipefail

[[ $# == 0 ]] || { printf 'Usage: bash ci/publish-docker.sh\n' >&2; exit 2; }
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(git -C "$SCRIPT_DIR/.." rev-parse --show-toplevel)
cd "$ROOT"

require_clean_develop() {
    local status
    [[ $(git symbolic-ref --quiet --short HEAD) == develop ]] || {
        printf 'Docker publication requires a develop checkout (not a tag or detached HEAD).\n' >&2; return 1;
    }
    status=$(git status --porcelain --untracked-files=all --ignore-submodules=none) || return
    [[ -z $status ]] || {
        printf 'Docker publication requires a clean checkout, including staged and untracked files.\n' >&2; return 1;
    }
    [[ $(git rev-parse HEAD) == "$REVISION" ]] || {
        printf 'Source revision changed during qualification; run publication again.\n' >&2; return 1;
    }
}

REVISION=$(git rev-parse HEAD)
require_clean_develop
: "${DOCKER_BIN:=docker}"
DOCKER_EXEC=$(command -v -- "$DOCKER_BIN" 2>/dev/null) || DOCKER_EXEC=''
if [[ ! -f $DOCKER_EXEC || ! -x $DOCKER_EXEC ]]; then
    printf 'Docker CLI unavailable: DOCKER_BIN=%s. Set DOCKER_BIN to an executable Docker CLI path or command.\n' "$DOCKER_BIN" >&2
    exit 127
fi
export DOCKER_BIN

# A fresh run prevents reusing receipts from an earlier or failed qualification.
read -r RUN_ID < /proc/sys/kernel/random/uuid
export RUN_ID
ART="$ROOT/.ci-artifacts/$RUN_ID"
printf 'Qualifying %s before Docker-only publication (run %s).\n' "$REVISION" "$RUN_ID"
bash "$SCRIPT_DIR/qualify.sh"
require_clean_develop

IMAGE_ID=$(< "$ART/qualified-image-id")
[[ $IMAGE_ID =~ ^sha256:[a-f0-9]{64}$ ]] || { printf 'Invalid qualified image ID.\n' >&2; exit 1; }
[[ $(< "$ART/image-id") == "$IMAGE_ID" && $(< "$ART/revision") == "$REVISION" ]] || {
    printf 'Qualification receipt does not match this image/revision.\n' >&2; exit 1;
}
[[ $("$DOCKER_BIN" image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$IMAGE_ID") == "$REVISION" ]] || {
    printf 'Qualified image revision does not match this checkout.\n' >&2; exit 1;
}
# Qualification verifies this label against the package.json inside the tested image.
VERSION=$("$DOCKER_BIN" image inspect --format '{{index .Config.Labels "org.opencontainers.image.version"}}' "$IMAGE_ID")
[[ $VERSION =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$ ]] || { printf 'Package version is not a valid Docker tag.\n' >&2; exit 1; }
export IMAGE=surikaterna/tapeworm-dispatcher VERSION
TAGS=$(bash "$SCRIPT_DIR/docker-tags.sh" local)

# The selector puts a missing version first and latest last; never rebuild here.
while IFS= read -r tag; do
    require_clean_develop
    "$DOCKER_BIN" tag "$IMAGE_ID" "$tag"
    "$DOCKER_BIN" push "$tag"
done <<< "$TAGS"
printf 'Published qualified image %s to Docker Hub.\n' "$IMAGE_ID"
