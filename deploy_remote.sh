#!/usr/bin/env bash
# GEKTOR APEX — client-side deploy launcher (run from your dev machine)
#
# This is the THIN client wrapper requested by ТЗ v2.0 Task 5. It does NOT
# replace the existing server-side deploy.sh — that one does the actual
# atomic blue-green release on the Tokyo box (Poetry, releases/<ts>/,
# symlink swap, systemctl reload). This wrapper just:
#
#   1. SSHes into the production server.
#   2. Triggers the existing /opt/gektor/current/deploy.sh.
#   3. Tails the service status so you see the result locally.
#
# Usage:
#   ./deploy_remote.sh                # deploy main
#   ./deploy_remote.sh feat/some-pr   # deploy a branch (must be pushed)
#
# Environment overrides:
#   GEKTOR_HOST     default: root@45.76.212.160 (Tokyo)
#   GEKTOR_BASE     default: /opt/gektor
#   GEKTOR_SERVICE  default: gektor.service
#   SSH_KEY         default: ~/.ssh/gektor_ed25519 (if exists, otherwise system default)
#
# Safety:
#   * Requires the target branch to be pushed to origin.
#   * Aborts on first error (set -euo pipefail).
#   * Never commits secrets — .env stays only on the server.

set -euo pipefail

REMOTE_HOST="${GEKTOR_HOST:-root@45.76.212.160}"
REMOTE_BASE="${GEKTOR_BASE:-/opt/gektor}"
REMOTE_SVC="${GEKTOR_SERVICE:-gektor.service}"
BRANCH="${1:-main}"
SSH_KEY_DEFAULT="${HOME}/.ssh/gektor_ed25519"
SSH_ARGS=()
if [[ -f "${SSH_KEY:-${SSH_KEY_DEFAULT}}" ]]; then
    SSH_ARGS+=("-i" "${SSH_KEY:-${SSH_KEY_DEFAULT}}")
fi

echo "[deploy_remote] target=${REMOTE_HOST} branch=${BRANCH}"

# 1. Sanity check: branch is pushed
if ! git ls-remote --heads origin "${BRANCH}" | grep -q .; then
    echo "[deploy_remote] FATAL: branch '${BRANCH}' is not on origin. Push first." >&2
    exit 1
fi

# 2. Run the server-side deploy
# shellcheck disable=SC2029
ssh "${SSH_ARGS[@]}" "${REMOTE_HOST}" "
    set -e
    cd ${REMOTE_BASE}/current
    echo '[deploy_remote] fetching latest ${BRANCH}...'
    git fetch origin
    git checkout ${BRANCH}
    git reset --hard origin/${BRANCH}
    echo '[deploy_remote] running server-side deploy.sh...'
    bash ./deploy.sh
"

# 3. Tail the service status
echo "[deploy_remote] post-deploy status:"
ssh "${SSH_ARGS[@]}" "${REMOTE_HOST}" "systemctl status ${REMOTE_SVC} --no-pager --lines=20" || true

echo "[deploy_remote] done."
