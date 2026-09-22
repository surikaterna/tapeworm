#!/usr/bin/env bash

# This report is deliberately allowlisted: never replace it with an environment dump.
set +x
set +e

: "${DOCKER_BIN:=docker}"
PHASE=${1:-qualification}

value() {
    local name=$1 data=${2:-}
    printf 'DIAGNOSTIC %s=' "$name"
    printf '%q\n' "$data"
}

probe_path() {
    local name=$1 command=$2 resolved
    resolved=$(command -v -- "$command" 2>/dev/null) || resolved='unavailable'
    value "$name" "$resolved"
}

docker_candidates() {
    local directory candidate canonical
    local -A seen=()
    while IFS= read -r directory; do
        [[ -n $directory ]] || directory=.
        candidate=$directory/docker
        [[ -f $candidate && -x $candidate ]] || continue
        canonical=$(readlink -f -- "$candidate" 2>/dev/null) || canonical=$candidate
        [[ -z ${seen["$canonical"]+present} ]] || continue
        seen["$canonical"]=1
        value docker_path_candidate "$candidate"
    done < <(printf '%s' "${PATH:-}" | tr ':' '\n')
    ((${#seen[@]} > 0)) || value docker_path_candidate unavailable
}

docker_metadata() {
    local selected=$1 target canonical metadata
    if [[ -L $selected ]]; then
        target=$(readlink -- "$selected" 2>/dev/null) || target=unreadable
        value docker_symlink_target "$target"
    else
        value docker_symlink_target not-a-symlink
    fi
    canonical=$(readlink -f -- "$selected" 2>/dev/null) || canonical=unavailable
    value docker_canonical_path "$canonical"
    metadata=$(stat -Lc 'mode=%A uid=%u gid=%g size=%s type=%F' -- "$selected" 2>/dev/null) || metadata=unavailable
    value docker_metadata "$metadata"
}

safe_probe() {
    local name=$1
    shift
    local output status
    output=$("$@" 2>/dev/null)
    status=$?
    case $name in
        docker_version)
            [[ $output =~ ^client=[0-9A-Za-z._+-]+\ server=[0-9A-Za-z._+-]*$ ]] || output='[redacted-unexpected-output]'
            ;;
        docker_context_name)
            [[ $output =~ ^[0-9A-Za-z._+-]+$ ]] || output='[redacted-unexpected-output]'
            ;;
    esac
    value "$name" "$output"
    printf 'DIAGNOSTIC %s_status=%d\n' "$name" "$status"
}

docker_probes() {
    local selected=$1 ostype status
    safe_probe docker_version "$selected" version --format 'client={{.Client.Version}} server={{.Server.Version}}'
    safe_probe docker_context_name "$selected" context show
    ostype=$("$selected" info --format '{{.OSType}}' 2>/dev/null)
    status=$?
    [[ $ostype =~ ^[0-9A-Za-z._+-]+$ ]] || ostype='[redacted-unexpected-output]'
    value docker_ostype "$ostype"
    if ((status == 0)) && [[ $ostype == linux ]]; then
        printf 'DIAGNOSTIC linux_docker_capability=PASS exit_status=0\n'
    else
        printf 'DIAGNOSTIC linux_docker_capability=FAIL exit_status=%d\n' "$status"
    fi
}

main() {
    local selected
    printf 'DIAGNOSTIC phase=%q\n' "$PHASE"
    value NODE_NAME "${NODE_NAME:-unset}"
    value NODE_LABELS "${NODE_LABELS:-unset}"
    value WORKSPACE "${WORKSPACE:-unset}"
    value current_directory "$PWD"
    value PATH "${PATH:-}"
    value docker_requested "$DOCKER_BIN"
    selected=$(command -v -- "$DOCKER_BIN" 2>/dev/null) || selected=unavailable
    value docker_selected "$selected"
    docker_candidates
    probe_path git_path git
    probe_path timeout_path timeout
    if [[ $selected == unavailable ]]; then
        if [[ -L $DOCKER_BIN ]]; then
            docker_metadata "$DOCKER_BIN"
        else
            value docker_symlink_target unavailable
            value docker_canonical_path unavailable
            value docker_metadata unavailable
        fi
        printf 'DIAGNOSTIC linux_docker_capability=FAIL exit_status=127\n'
        return
    fi
    docker_metadata "$selected"
    if [[ ! -f $selected || ! -x $selected ]]; then
        printf 'DIAGNOSTIC linux_docker_capability=FAIL exit_status=126\n'
        return
    fi
    docker_probes "$selected"
}

main
exit 0
