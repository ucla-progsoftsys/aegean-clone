#!/usr/bin/env bash
set -euo pipefail

if [[ "${ENABLE_REDIS:-0}" == "1" ]]; then
  mkdir -p /var/lib/redis /var/log/redis
  redis-server \
    --bind "${REDIS_BIND:-0.0.0.0}" \
    --port "${REDIS_PORT:-6379}" \
    --dir /var/lib/redis \
    --appendonly "${REDIS_APPENDONLY:-no}" \
    --daemonize yes
fi

exec /usr/sbin/sshd -D
