#!/bin/sh
# Run only after the separately approved installation. Default is read-only.
set -eu
: "${AIRLAKE_OPS_PACKET:?Set the private operations packet directory}"
ops() {
    uv run --frozen --offline --with 'botocore[crt]==1.43.115' \
        python -B -m live_data.operations "$@" --packet "$AIRLAKE_OPS_PACKET"
}
ops preflight --installed
if [ "${AIRLAKE_OPS_APPROVED:-no}" != yes ]; then
    printf '%s\n' 'PASS: read-only preflight; STOP: deployment/demo writes require exact-plan approval.'
    exit 0
fi
: "${AIRLAKE_OPS_PLAN_SHA256:?Set the separately approved operations plan hash}"
stop() {
    ops stop --execute --confirm-plan-sha256 "$AIRLAKE_OPS_PLAN_SHA256"
}
finish() {
    _airlake_status=$?
    trap - EXIT
    stop || _airlake_status=1
    exit "$_airlake_status"
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP
ops enable --execute --confirm-plan-sha256 "$AIRLAKE_OPS_PLAN_SHA256"
ops watch --execute --confirm-plan-sha256 "$AIRLAKE_OPS_PLAN_SHA256"
# Preserve the first final evidence; never overwrite a prior run.
( set -C; ops status > "$AIRLAKE_OPS_PACKET/demo-final-status.json" )
ops http --execute --confirm-plan-sha256 "$AIRLAKE_OPS_PLAN_SHA256"
printf '%s\n' 'STOPPED: inspect final status and API receipt; NO_NEW_DATA is not a Gold-refresh PASS.'
