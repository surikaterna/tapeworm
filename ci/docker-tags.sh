#!/usr/bin/env bash
set -euo pipefail

: "${DOCKER_BIN:=docker}"
mode=${1:-github}
[[ $mode == github || $mode == local ]] || { printf 'Usage: docker-tags.sh [github|local]\n' >&2; exit 2; }
reference="docker.io/${IMAGE:?}:${VERSION:?}"
version_tag=""

if result=$("$DOCKER_BIN" manifest inspect "$reference" 2>&1); then
  printf 'Keeping existing image %s\n' "$reference" >&2
elif [[ "$result" == "no such manifest: $reference" ]]; then
  version_tag="$IMAGE:$VERSION"
else
  # Authentication, network, and registry errors must not permit overwriting a tag.
  printf '%s\n' "$result" >&2
  exit 1
fi

if [[ $mode == local ]]; then
  if [[ -n "$version_tag" ]]; then printf '%s\n' "$version_tag"; fi
  printf '%s\n' "$IMAGE:latest"
  exit
fi

printf 'tags<<EOF\n'
printf '%s\n' "$IMAGE:develop-${RUN_NUMBER:?}" "$IMAGE:latest"
if [[ -n "$version_tag" ]]; then
  printf '%s\n' "$version_tag"
fi
printf 'EOF\n'
