#!/usr/bin/env bash
set -Eeuo pipefail
: "${RUN_ID:?}" "${NET:?}" "${LABEL:?}" "${PROJECT:?}" "${ROOT:?}" "${ART:?}"
: "${DOCKER_BIN:=docker}"
DOCKER_EXEC=$(command -v -- "$DOCKER_BIN" 2>/dev/null) || DOCKER_EXEC=''
if [[ ! -f $DOCKER_EXEC || ! -x $DOCKER_EXEC ]]; then
    printf 'Docker CLI unavailable: DOCKER_BIN=%s. Set DOCKER_BIN to an executable Docker CLI path or command.\n' "$DOCKER_BIN" >&2
    exit 127
fi
export DOCKER_BIN

mongo_ready() {
    local name=$1 end=$((SECONDS + 90))
    until "$DOCKER_BIN" exec "$name" mongosh --quiet --eval 'quit(db.adminCommand({ping:1}).ok === 1 ? 0 : 1)' >/dev/null 2>&1; do
        (( SECONDS < end )) || return 1
        sleep 1
    done
    "$DOCKER_BIN" exec "$name" mongosh --quiet --eval 'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})'
    until "$DOCKER_BIN" exec "$name" mongosh --quiet --eval 'quit(db.hello().isWritablePrimary ? 0 : 1)' >/dev/null 2>&1; do
        (( SECONDS < end )) || return 1
        sleep 1
    done
}

start() {
    "$DOCKER_BIN" network create --label "$LABEL" --label "$PROJECT" "$NET"
    local service
    for service in mongo expiry; do
        local options=()
        [[ $service == expiry ]] && options=(--oplogSize 1 --syncdelay 1)
        "$DOCKER_BIN" volume create --label "$LABEL" --label "$PROJECT" "tw-$RUN_ID-$service-data"
        "$DOCKER_BIN" volume create --label "$LABEL" --label "$PROJECT" "tw-$RUN_ID-$service-config"
        "$DOCKER_BIN" run -d --name "tw-$RUN_ID-$service" --label "$LABEL" --label "$PROJECT" \
            --mount "type=volume,src=tw-$RUN_ID-$service-data,dst=/data/db" \
            --mount "type=volume,src=tw-$RUN_ID-$service-config,dst=/data/configdb" \
            --network "$NET" --network-alias "$service" mongo:8.3.9 \
            --replSet rs0 --bind_ip_all "${options[@]}"
        mongo_ready "tw-$RUN_ID-$service"
    done
    "$DOCKER_BIN" run -d --name "tw-$RUN_ID-rabbit" --label "$LABEL" --label "$PROJECT" \
        --user 999:999 --hostname rabbit --tmpfs /var/lib/rabbitmq:rw,uid=999,gid=999,mode=0700 \
        --network "$NET" --network-alias rabbit -e RABBITMQ_DEFAULT_USER=ci \
        -e RABBITMQ_DEFAULT_PASS=ci-test-only rabbitmq:4.3.6
    local end=$((SECONDS + 90))
    until "$DOCKER_BIN" exec "tw-$RUN_ID-rabbit" rabbitmq-diagnostics -q check_running >/dev/null 2>&1; do
        [[ $("$DOCKER_BIN" inspect --format '{{.State.Running}}' "tw-$RUN_ID-rabbit") == true ]] || return 1
        (( SECONDS < end )) || return 1
        sleep 1
    done
    [[ $("$DOCKER_BIN" exec "tw-$RUN_ID-rabbit" rabbitmqctl version) == 4.3.6 ]]
}

probe() {
    "$DOCKER_BIN" run --rm --name "tw-$RUN_ID-probe" --label "$LABEL" --label "$PROJECT" \
        --network "$NET" --entrypoint node \
        --mount "type=bind,src=$ROOT/ci,dst=/app/ci,readonly" \
        --mount "type=bind,src=$ART,dst=/evidence,readonly" \
        "$IMAGE" /app/ci/image-smoke.mjs "$@"
}

worker() {
    local name=$1 database=$2
    "$DOCKER_BIN" run -d --name "tw-$RUN_ID-$name" --label "$LABEL" --label "$PROJECT" \
        --network "$NET" -e MONGODB_URI='mongodb://mongo:27017/?directConnection=true&replicaSet=rs0' \
        -e RABBITMQ_URI='amqp://ci:ci-test-only@rabbit:5672' -e DATABASE=bad.env.name \
        -e COLLECTION=commits -e EXCHANGE=ci_smoke -e RESUME_COLLECTION=checkpoint \
        -e CHECKPOINT_KEY=ci-smoke -e FEED_ID=ci-smoke "$IMAGE" --database "$database"
}

wait_exit() {
    local name=$1 expected=$2 code
    code=$(timeout 25 "$DOCKER_BIN" wait "tw-$RUN_ID-$name")
    "$DOCKER_BIN" logs --tail 100 "tw-$RUN_ID-$name" > "$ART/$name.log" 2>&1
    [[ $code == "$expected" ]] || { printf '%s: expected %s, got %s\n' "$name" "$expected" "$code"; return 1; }
    printf 'IMAGE EXIT %s=%s\n' "$name" "$code"
}

stop_worker() {
    local name=$1 expected=$2 start elapsed
    start=$(date +%s%3N)
    timeout 35 "$DOCKER_BIN" stop --time 30 "tw-$RUN_ID-$name"
    elapsed=$(($(date +%s%3N) - start))
    wait_exit "$name" "$expected"
    if [[ $expected == 124 ]]; then (( elapsed >= 9500 && elapsed < 25000 )); fi
    printf 'IMAGE STOP %s elapsed=%sms externalGrace=30s\n' "$name" "$elapsed"
}

unlock() {
    "$DOCKER_BIN" exec "tw-$RUN_ID-mongo" mongosh --quiet --eval \
        'if(db.adminCommand({currentOp:1}).fsyncLock) printjson(db.adminCommand({fsyncUnlock:1})); if(db.adminCommand({currentOp:1}).fsyncLock) quit(1)'
}

smoke() {
    printf '\nGATE actual production image smoke %s\n' "$IMAGE"
    [[ $("$DOCKER_BIN" image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$IMAGE") == "$(< "$ART/revision")" ]]
    [[ $("$DOCKER_BIN" run --rm --label "$LABEL" --label "$PROJECT" --entrypoint npm "$IMAGE" --version) == 11.12.1 ]]
    [[ $("$DOCKER_BIN" image inspect --format '{{index .Config.Labels "org.opencontainers.image.version"}}' "$IMAGE") == "$("$DOCKER_BIN" run --rm --label "$LABEL" --label "$PROJECT" --entrypoint node "$IMAGE" -p 'require("./packages/tapeworm_dispatcher_mdb_rmq/package.json").version')" ]]
    probe identity
    probe prepare
    worker normal ci_smoke
    "$DOCKER_BIN" exec "tw-$RUN_ID-normal" node -e 'const f=require("node:fs"),a=require("node:assert/strict");a.notEqual(process.getuid(),0);const pid1=f.readFileSync("/proc/1/cmdline","utf8").replaceAll("\0"," ");a.match(pid1,/tini/);const child=f.readFileSync("/proc/1/task/1/children","utf8").trim().split(" ")[0];const cmd=f.readFileSync(`/proc/${child}/cmdline`,"utf8").replaceAll("\0"," ");a.match(cmd,/^node packages\/tapeworm_dispatcher_mdb_rmq\/dist\/bin\/cli.js/);console.log({pid1,child,cmd});'
    probe delivered
    stop_worker normal 0
    worker invalid bad.name
    wait_exit invalid 1
    probe prepare-lock
    trap unlock EXIT
    probe lock
    worker blocked ci_smoke
    probe blocked
    stop_worker blocked 124
    probe locked
    unlock
    trap - EXIT
    probe next
    worker restarted ci_smoke
    probe progressed
    stop_worker restarted 0
}

case ${1:?} in start) start ;; smoke) smoke ;; *) exit 2 ;; esac
